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


import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.stage.origin.grpc.Groups;

@StageDef(
        version = 1,
        label = "Grpc Processor",
        description = "GRPC Client",
        icon = "grpc.png",
        execution = ExecutionMode.STANDALONE,
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class GrpcDProcessor extends DProcessor {
    @ConfigDefBean()
    public ConfigBean conf;

    @Override
    protected Processor createProcessor() {
        return new GrpcProcessor(conf);
    }
}
