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
package com.streamsets.stage.lib.grpc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.stage.lib.grpc.grpc.GrpcConfigBean;
import com.streamsets.stage.lib.grpc.grpc.ServerReflectionClient;
import com.streamsets.stage.processor.grpc.Groups;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class Utils {

  private Utils(){
    throw new IllegalStateException("Utility class");
  }

  /**
   * Returns a {@link DescriptorProtos.FileDescriptorSet} describing the supplied service if the remote server
   * advertizes it by reflection. Returns an empty optional if the remote server doesn't support
   * reflection. Throws a NOT_FOUND exception if we determine that the remote server does not
   * support the requested service (but *does* support the reflection service).
   */
  public static Optional<DescriptorProtos.FileDescriptorSet> resolveServiceByReflection(
          Channel channel, String serviceName, List<Stage.ConfigIssue> issues, Stage.Context context) {
    final String CONFIG_NAME = "useReflection";

    ServerReflectionClient serverReflectionClient = ServerReflectionClient.create(channel);
    ImmutableList<String> services;
    try {
      services = serverReflectionClient.listServices().get();
    } catch (StatusRuntimeException e) {
      // Listing services failed, try and provide an explanation.
      if (e.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
        issues.add(context.createConfigIssue(
                Groups.GRPC.name(),
                CONFIG_NAME,
                Errors.GRPC_03));
      }

      // In any case, return an empty optional to indicate that this failed.
      return Optional.empty();
    } catch (Exception e) {
      issues.add(context.createConfigIssue(
              Groups.GRPC.name(),
              CONFIG_NAME,
              Errors.GRPC_04,
              e));
      return Optional.empty();
    }

    if (!services.contains(serviceName)) {
      throw Status.NOT_FOUND
              .withDescription(String.format(
                      "Remote server does not have service %s. Services: %s", serviceName, services))
              .asRuntimeException();
    }

    try {
      return Optional.of(serverReflectionClient.lookupService(serviceName).get());
    } catch (Exception e) {
      issues.add(context.createConfigIssue(
              Groups.GRPC.name(),
              CONFIG_NAME,
              Errors.GRPC_05,
              e));
      return Optional.empty();
    }
  }

  public static CallOptions callOptions(GrpcConfigBean conf) {
    CallOptions result = CallOptions.DEFAULT;
    if (conf.deadlineMs > 0) {
      result = result.withDeadlineAfter(conf.deadlineMs, TimeUnit.MILLISECONDS);
    }
    return result;
  }

  public static void validatePath(Optional<Path> maybePath) {
    if (maybePath.isPresent()) {
      Preconditions.checkArgument(maybePath.get().toFile().exists());
    }
  }

  public static void validatePaths(Iterable<Path> paths) {
    for (Path path : paths) {
      Preconditions.checkArgument(path.toFile().exists());
    }
  }
}
