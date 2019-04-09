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
package com.streamsets.stage.lib.grpc.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.streamsets.stage.lib.grpc.io.MessageWriter;
import com.streamsets.stage.lib.grpc.protobuf.DynamicMessageMarshaller;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A grpc client which operates on dynamic messages. */
public class DynamicGrpcClient {
  private static final Logger logger = LoggerFactory.getLogger(DynamicGrpcClient.class);
  private final MethodDescriptor protoMethodDescriptor;
  private final Channel channel;

  /** Creates a client for the supplied method, talking to the supplied endpoint. */
  public static DynamicGrpcClient create(MethodDescriptor protoMethod, Channel channel) {
    return new DynamicGrpcClient(protoMethod, channel);
  }

  @VisibleForTesting
  DynamicGrpcClient(MethodDescriptor protoMethodDescriptor, Channel channel) {
    this.protoMethodDescriptor = protoMethodDescriptor;
    this.channel = channel;
  }

  /**
   * Makes an rpc to the remote endpoint and respects the supplied callback. Returns a future which
   * terminates once the call has ended. For calls which are single-request, this throws
   * {@link IllegalArgumentException} if the size of {@code requests} is not exactly 1.
   */
  public ListenableFuture<String> call(
          ImmutableList<DynamicMessage> requests,
          MessageWriter responseObserver,
          CallOptions callOptions) {
    Preconditions.checkArgument(!requests.isEmpty(), "Can't make call without any requests");
    MethodType methodType = getMethodType();
    long numRequests = requests.size();
    if (methodType == MethodType.UNARY) {
      logger.info("Making unary call");
      Preconditions.checkArgument(numRequests == 1,
              "Need exactly 1 request for unary call, but got: " + numRequests);
      return callUnary(requests.get(0), responseObserver, callOptions);
    } else if (methodType == MethodType.SERVER_STREAMING) {
      logger.info("Making server streaming call");
      Preconditions.checkArgument(numRequests == 1,
              "Need exactly 1 request for server streaming call, but got: " + numRequests);
      return callServerStreaming(requests.get(0), responseObserver, callOptions);
    } else if (methodType == MethodType.CLIENT_STREAMING) {
      logger.info("Making client streaming call with {} requests", requests.size());
      return callClientStreaming(requests, responseObserver, callOptions);
    } else {
      // Bidi streaming.
      logger.info("Making bidi streaming call with {} requests", requests.size());
      return callBidiStreaming(requests, responseObserver, callOptions);
    }
  }

  private ListenableFuture<String> callBidiStreaming(
          ImmutableList<DynamicMessage> requests,
          MessageWriter responseObserver,
          CallOptions callOptions) {

    StreamObserver<DynamicMessage> requestObserver = ClientCalls.asyncBidiStreamingCall(
            createCall(callOptions),
            responseObserver);
    requests.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    return responseObserver.getResultFuture();
  }

  private ListenableFuture<String> callClientStreaming(
          ImmutableList<DynamicMessage> requests,
          MessageWriter responseObserver,
          CallOptions callOptions) {

    StreamObserver<DynamicMessage> requestObserver = ClientCalls.asyncClientStreamingCall(
            createCall(callOptions),
            responseObserver);
    requests.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    return responseObserver.getResultFuture();
  }

  private ListenableFuture<String> callServerStreaming(
          DynamicMessage request,
          MessageWriter responseObserver,
          CallOptions callOptions) {

    ClientCalls.asyncServerStreamingCall(
            createCall(callOptions),
            request,
            responseObserver);
    return responseObserver.getResultFuture();
  }

  private ListenableFuture<String> callUnary(
          DynamicMessage request,
          MessageWriter responseObserver,
          CallOptions callOptions) {

    ClientCalls.asyncUnaryCall(
            createCall(callOptions),
            request,
            responseObserver);
    return responseObserver.getResultFuture();
  }

  private ClientCall<DynamicMessage, DynamicMessage> createCall(CallOptions callOptions) {
    return channel.newCall(createGrpcMethodDescriptor(), callOptions);
  }

  private io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> createGrpcMethodDescriptor() {
    return io.grpc.MethodDescriptor.<DynamicMessage, DynamicMessage>create(
        getMethodType(),
        getFullMethodName(),
        new DynamicMessageMarshaller(protoMethodDescriptor.getInputType()),
        new DynamicMessageMarshaller(protoMethodDescriptor.getOutputType()));
  }

  private String getFullMethodName() {
    String serviceName = protoMethodDescriptor.getService().getFullName();
    String methodName = protoMethodDescriptor.getName();
    return io.grpc.MethodDescriptor.generateFullMethodName(serviceName, methodName);
  }

  /** Returns the appropriate method type based on whether the client or server expect streams. */
  public MethodType getMethodType() {
    boolean clientStreaming = protoMethodDescriptor.toProto().getClientStreaming();
    boolean serverStreaming = protoMethodDescriptor.toProto().getServerStreaming();

    if (!clientStreaming && !serverStreaming) {
      return MethodType.UNARY;
    } else if (!clientStreaming && serverStreaming) {
      return MethodType.SERVER_STREAMING;
    } else if (clientStreaming && !serverStreaming) {
      return MethodType.CLIENT_STREAMING;
    } else {
      return MethodType.BIDI_STREAMING;
    }
  }
}
