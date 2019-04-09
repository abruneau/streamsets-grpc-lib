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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class GrpcConfigBean {

    // The flags below represent overrides for the configuration used at runtime.

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            defaultValue = "true",
            label = "Use Reflection",
            description = "Try to use reflection first to resolve protos (default: true)",
            group = "#0",
            displayPosition = 1
    )
    public boolean useReflection = true;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Proto Discovery Root",
            description = "Root directory to scan for proto files",
            group = "#0",
            dependencies = {
                    @Dependency(configName = "useReflection", triggeredByValues = "false"),
            },
            displayPosition = 2
    )
    public String protoDiscoveryRootArg = null;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Full Method",
            description = "Full name of the method to call: <some.package.Service/doSomething>",
            group = "#0",
            displayPosition = 3
    )
    public String fullMethodArg;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Endpoint",
            description = "Service endpoint to call: <host>:<port>\"",
            group = "#0",
            displayPosition = 4
    )
    public String endpointArg;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.TEXT,
            label = "Request",
            elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
            evaluation = ConfigDef.Evaluation.EXPLICIT,
            description = "Grpc request",
            group = "#0",
            displayPosition = 5
    )
    public String request;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.LIST,
            label = "Add Protoc Includes",
            description = "Comma separated list of protoc path to include",
            group = "#0",
            displayPosition = 6
    )
    public List<String> addProtocIncludes = null;


    @ConfigDef(
            required = false,
            type = ConfigDef.Type.NUMBER,
            label = "Deadline Ms",
            description = "How long to wait for a call to complete (see gRPC doc)",
            group = "#0",
            displayPosition = 7
    )
    public Integer deadlineMs;

    /** Returns the root of the directory tree in which to discover proto files. */
    public Optional<Path> protoDiscoveryRoot() {
        return maybeInputPath(protoDiscoveryRootArg);
    }

    /** Defaults to true. */
    public boolean useReflection() {
        return useReflection;
    }

    public ImmutableList<Path> additionalProtocIncludes() {
        if (addProtocIncludes == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<Path> resultBuilder = ImmutableList.builder();
        for (String pathString : addProtocIncludes) {
            Path includePath = Paths.get(pathString);
            Preconditions.checkArgument(includePath.toFile().exists(), "Invalid include: " + includePath);
            resultBuilder.add(includePath);
        }
        return resultBuilder.build();
    }

    // *************************************
    // * Flags supporting the call command *
    // *************************************

    /** Returns the endpoint string */
    public Optional<String> endpoint() {
        return Optional.ofNullable(endpointArg);
    }

    /** Returns the endpoint method */
    public Optional<String> fullMethod() {
        return Optional.ofNullable(fullMethodArg);
    }

    // ******************
    // * Helper methods *
    // ******************
    private static Optional<Path> maybeOutputPath(String rawPath) {
        if (rawPath == null) {
            return Optional.empty();
        }
        return Optional.of(Paths.get(rawPath));
    }

    private static Optional<Path> maybeInputPath(String rawPath) {
        if (rawPath == null || rawPath == "") {
            return Optional.empty();
        }
        return maybeOutputPath(rawPath).map(path -> {
            Preconditions.checkArgument(path.toFile().exists(), "File " + rawPath + " does not exist");
            return path;
        });
    }
}
