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
package com.streamsets.stage.lib.grpc.protobuf;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.*;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;

/**
 * Central place to store information about the protobuf well-known-types.
 */
public class WellKnownTypes {

  private  WellKnownTypes(){
    throw new IllegalStateException("Utility class");
  }

  private static final ImmutableSet<FileDescriptorProto> DESCRIPTORS = ImmutableSet.of(
      AnyProto.getDescriptor().getFile().toProto(),
      ApiProto.getDescriptor().getFile().toProto(),
      DescriptorProto.getDescriptor().getFile().toProto(),
      DurationProto.getDescriptor().getFile().toProto(),
      EmptyProto.getDescriptor().getFile().toProto(),
      FieldMaskProto.getDescriptor().getFile().toProto(),
      SourceContextProto.getDescriptor().getFile().toProto(),
      StructProto.getDescriptor().getFile().toProto(),
      TimestampProto.getDescriptor().getFile().toProto(),
      TypeProto.getDescriptor().getFile().toProto(),
      WrappersProto.getDescriptor().getFile().toProto());

  private static final ImmutableSet<String> FILES = ImmutableSet.of(
      "any.proto",
      "api.proto",
      "descriptor.proto",
      "duration.proto",
      "empty.proto",
      "field_mask.proto",
      "source_context.proto",
      "struct.proto",
      "timestamp.proto",
      "type.proto",
      "wrappers.proto");

  public static ImmutableSet<FileDescriptorProto> descriptors() {
    return DESCRIPTORS;
  }

  public static ImmutableSet<String> fileNames() {
    return FILES;
  }
}
