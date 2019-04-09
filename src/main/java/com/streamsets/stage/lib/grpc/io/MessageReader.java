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
package com.streamsets.stage.lib.grpc.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import java.io.BufferedReader;
import java.io.StringReader;

/** A utility class which knows how to read proto files written using {@link MessageWriter}. */
public class MessageReader {
  private final JsonFormat.Parser jsonParser;
  private final Descriptor descriptor;
  private final BufferedReader bufferedReader;
  private final String source;

  /** Creates a {@link MessageReader} which reads messages from string. */
  public static MessageReader forString(String source, Descriptor descriptor, TypeRegistry registry) {
    BufferedReader reader = new BufferedReader(new StringReader(source));
    return new MessageReader(
            JsonFormat.parser().usingTypeRegistry(registry),
            descriptor,
            reader,
            "Request"
    );
  }

  @VisibleForTesting
  MessageReader(
      JsonFormat.Parser jsonParser,
      Descriptor descriptor,
      BufferedReader bufferedReader,
      String source) {
    this.jsonParser = jsonParser;
    this.descriptor = descriptor;
    this.bufferedReader = bufferedReader;
    this.source = source;
  }

  /** Parses all the messages and returns them in a list. */
  public ImmutableList<DynamicMessage> read() throws IllegalArgumentException {
    ImmutableList.Builder<DynamicMessage> resultBuilder = ImmutableList.builder();
    try {
      String line;
      boolean wasLastLineEmpty = false;
      while (true) {
        line = bufferedReader.readLine();

        // Two consecutive empty lines mark the end of the stream.
        if (Strings.isNullOrEmpty(line)) {
          if (wasLastLineEmpty) {
            return resultBuilder.build();
          }
          wasLastLineEmpty = true;
          continue;
        }

        // Read the next full message.
        StringBuilder stringBuilder = new StringBuilder();
        while (!Strings.isNullOrEmpty(line)) {
          stringBuilder.append(line);
          line = bufferedReader.readLine();
        }
        wasLastLineEmpty = true;

        DynamicMessage.Builder nextMessage = DynamicMessage.newBuilder(descriptor);
        jsonParser.merge(stringBuilder.toString(), nextMessage);

        // Clean up and prepare for next message.
        resultBuilder.add(nextMessage.build());
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }
}
