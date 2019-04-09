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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;

import io.grpc.stub.StreamObserver;

import java.util.*;

public class MessageWriter implements StreamObserver<DynamicMessage> {

  private final SettableFuture<String> resultFuture;

  private final BatchMaker batchMaker;
  private final Stage.Context context;

  enum StageType {ORIGINE, PROCESSOR}
  private StageType stageType;


  // Origin
  private int maxBatchSize;
  private long nextSourceOffset = 0;
  private int numRecords = 0;

  // Processor
  private Record record;
  private String targetField;
  private List<Field> responses = new ArrayList();

  public static MessageWriter create(Stage.Context context, BatchMaker batchMaker, Record record, String targetField){
    return new MessageWriter(context, batchMaker, null, 0, record, targetField, StageType.PROCESSOR);
  }

  public static MessageWriter create(Stage.Context context, BatchMaker batchMaker, String lastSourceOffset, int maxBatchSize){
    return new MessageWriter(context, batchMaker, lastSourceOffset, maxBatchSize, null, null, StageType.ORIGINE);
  }

  private MessageWriter(Stage.Context context,
                        BatchMaker batchMaker,
                        String lastSourceOffset,
                        int maxBatchSize,
                        Record record,
                        String targetField,
                        StageType stageType
  ) {
    this.batchMaker = batchMaker;
    this.context = context;

    this.stageType = stageType;

    if (this.stageType == StageType.ORIGINE) {
      this.maxBatchSize = maxBatchSize;
      if (lastSourceOffset != null) {
        nextSourceOffset = Long.parseLong(lastSourceOffset);
      }
    }

    if (this.stageType == StageType.PROCESSOR) {
      this.record = record;
      this.targetField = targetField;
    }

    resultFuture = SettableFuture.create();
  }


  @Override
  public void onNext(DynamicMessage message) {
    switch (stageType) {
      case ORIGINE:
        onNextOrigin(message);
        break;
      case PROCESSOR:
        onNextProcessor(message);
        break;
    }
  }

  private void onNextOrigin(DynamicMessage message) {
    Record rec = context.createRecord("record::" + nextSourceOffset);
    if ( numRecords < maxBatchSize) {
      Map<String, Field> map = parseDynamicMessage(message);
      rec.set(Field.create(map));
      batchMaker.addRecord(rec);
      ++nextSourceOffset;
      ++numRecords;
    }
    if (numRecords == maxBatchSize) {
      resultFuture.set(String.valueOf(nextSourceOffset));
      onCompleted();
    }

  }

  private void onNextProcessor(DynamicMessage message) {
    Map<String, Field> map = parseDynamicMessage(message);
    record.set(targetField, Field.create(map));
    responses.add(Field.create(map));
  }



  @Override
  public void onError(Throwable t) {
    resultFuture.setException(new RuntimeException("Error while processing messages", t.getCause()));
  }

  @Override
  public void onCompleted() {
    if (stageType == StageType.PROCESSOR) {
      onCompletedProcessor();
    }
    if (!resultFuture.isDone()) {
      resultFuture.set(String.valueOf(nextSourceOffset));
    }
  }

  private void onCompletedProcessor() {
    if (responses.size() == 1){
      record.set(targetField, responses.get(0));
    } else {
      record.set(targetField, Field.create(responses));
    }
    batchMaker.addRecord(record);
    resultFuture.set(String.valueOf((responses.size())));
  }

  public ListenableFuture<String> getResultFuture() {
    return resultFuture;
  }

  private static Map<String, Field> parseDynamicMessage (DynamicMessage message) {
    Map<String, Field> map = new HashMap<>();
    Iterator fields = message.getAllFields().entrySet().iterator();
    while (fields.hasNext()) {
      Map.Entry<Descriptors.FieldDescriptor,Object> field = (Map.Entry)fields.next();

      if (field.getValue().getClass() == DynamicMessage.class){
        map.put(field.getKey().getName(), Field.create(parseDynamicMessage((DynamicMessage)field.getValue())));
      }else {

        map.put(field.getKey().getName(), cast(field));
      }

    }
    return map;
  }

  private static Field cast (Map.Entry<Descriptors.FieldDescriptor,Object> field) {
    Field message;
    switch (field.getKey().getJavaType()) {
      case STRING: message = Field.create(field.getValue().toString());
        break;
      case INT: message = Field.create((int)field.getValue());
        break;
      case LONG: message = Field.create((long) field.getValue());
        break;
      case FLOAT: message = Field.create((float) field.getValue());
        break;
      case DOUBLE: message = Field.create((double) field.getValue());
        break;
      case BOOLEAN: message = Field.create((boolean) field.getValue());
        break;
      case BYTE_STRING: message = Field.create((byte[]) field.getValue());
        break;
      default: message = Field.create(field.getValue().toString());

    }
    return message;

  }
}
