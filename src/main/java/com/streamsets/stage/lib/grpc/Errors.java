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

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {

  GRPC_PROCESSOR_01("BIDI requests not supported"),
  GRPC_01("Failed to evaluate expression: '{}'"),
  GRPC_02("Unable to resolve service by invoking protoc: {}"),
  GRPC_03("Could not list services because the remote host does not support reflection."),
  GRPC_04("Could not list services: {}"),
  GRPC_05("Unable to lookup service by reflection: {}"),
  GRPC_06("Caught exception while waiting for rpc: {}"),
    ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}
