/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.stage.processor.grpc;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.stage.lib.grpc.Errors;
import com.streamsets.stage.lib.grpc.Utils;
import com.streamsets.stage.lib.grpc.grpc.ChannelFactory;
import com.streamsets.stage.lib.grpc.grpc.DynamicGrpcClient;
import com.streamsets.stage.lib.grpc.io.MessageReader;
import com.streamsets.stage.lib.grpc.io.MessageWriter;
import com.streamsets.stage.lib.grpc.protobuf.ProtoMethodName;
import com.streamsets.stage.lib.grpc.protobuf.ProtocInvoker;
import com.streamsets.stage.lib.grpc.protobuf.ServiceResolver;
import com.streamsets.stage.origin.grpc.GrpcSource;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class GrpcProcessor extends BaseProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcSource.class);
  private final ConfigBean conf;
  private Descriptors.MethodDescriptor methodDescriptor;
  private DynamicGrpcClient dynamicClient;

  private ELEval requestEval;

  private JsonFormat.TypeRegistry registry;

  public GrpcProcessor(ConfigBean grpcConfigBean) {this.conf = grpcConfigBean; }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    HostAndPort hostAndPort = HostAndPort.fromString(conf.grpcConfig.endpoint().orElse(""));
    ProtoMethodName grpcMethodName = ProtoMethodName.parseFullGrpcMethodName(conf.grpcConfig.fullMethod().orElse(""));
    Optional<Path> protoDiscoveryRoot = conf.grpcConfig.protoDiscoveryRoot();
    ImmutableList<Path> additionalProtocIncludes = conf.grpcConfig.additionalProtocIncludes();

    Utils.validatePath(protoDiscoveryRoot);
    Utils.validatePaths(additionalProtocIncludes);


    ChannelFactory channelFactory = ChannelFactory.create(conf.tlsConfig);

    LOG.debug("Creating channel to: {} ", hostAndPort);
    Channel channel;
    if (conf.oauthConfig.oauthEnabled) {
      channel = channelFactory.createChannelWithCredentials(hostAndPort, conf.oauthConfig.getCredentials());
    } else {
      channel = channelFactory.createChannel(hostAndPort);
    }


    // Fetch the appropriate file descriptors for the service.
    DescriptorProtos.FileDescriptorSet fileDescriptorSet;
    Optional<DescriptorProtos.FileDescriptorSet> reflectionDescriptors = Optional.empty();
    if (conf.grpcConfig.useReflection()) {
      reflectionDescriptors =
              Utils.resolveServiceByReflection(channel, grpcMethodName.getFullServiceName(), issues, getContext());
    }

    if (reflectionDescriptors.isPresent()) {
      LOG.debug("Using proto descriptors fetched by reflection");
      fileDescriptorSet = reflectionDescriptors.get();
    } else {
      try {
        fileDescriptorSet = ProtocInvoker.forConfig(conf.grpcConfig).invoke();
        LOG.debug("Using proto descriptors obtained from protoc");
      } catch (Exception e) {
        issues.add(getContext().createConfigIssue(
                Groups.GRPC.name(),
                "protoDiscoveryRootArg",
                Errors.GRPC_02,
                e
        ));
        return Collections.emptyList();
      }
    }

    // Set up the dynamic client and make the call.
    ServiceResolver serviceResolver = ServiceResolver.fromFileDescriptorSet(fileDescriptorSet);
    methodDescriptor = serviceResolver.resolveServiceMethod(grpcMethodName);

    LOG.debug("Creating dynamic grpc client");
    dynamicClient = DynamicGrpcClient.create(methodDescriptor, channel);

    if (dynamicClient.getMethodType() == MethodDescriptor.MethodType.BIDI_STREAMING){
      issues.add(getContext().createConfigIssue(
              Groups.GRPC.name(),
              "fullMethodArg",
              Errors.GRPC_PROCESSOR_01
      ));
    }

    // This collects all known types into a registry for resolution of potential "Any" types.
    registry = JsonFormat.TypeRegistry.newBuilder()
            .add(serviceResolver.listMessageTypes())
            .build();

    requestEval = getContext().createELEval("request");

    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();

    while (records.hasNext()) {
      Record record = records.next();

      String resolveRequest;
      try {
        ELVars elVars = getContext().createELVars();
        RecordEL.setRecordInContext(elVars, record);
        resolveRequest = requestEval.eval(elVars, conf.grpcConfig.request, String.class);
      } catch (ELEvalException e) {
        LOG.error(Errors.GRPC_01.getMessage(), conf.grpcConfig.request, e);
        throw new OnRecordErrorException(record, Errors.GRPC_01, conf.grpcConfig.request);
      }

      ImmutableList<DynamicMessage> requestMessages =
              MessageReader.forString(resolveRequest, methodDescriptor.getInputType(), registry).read();

      MessageWriter responseObserver = MessageWriter.create(getContext(), batchMaker, record, conf.targetField);

      ListenableFuture<String> resultFuture;

      try {
        resultFuture = dynamicClient.call(requestMessages, responseObserver, Utils.callOptions(conf.grpcConfig));
      } catch (Exception e) {
        throw new StageException(Errors.GRPC_06, e);
      }

      try {
        resultFuture.get();
      } catch (Exception e) {
        throw new StageException(Errors.GRPC_06, e);
      }

    }
  }
}
