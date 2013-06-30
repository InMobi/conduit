// GWT support for Protocol Buffers - Google's data interchange format
// Copyright 2011 Vitaliy Kulikov, vkulikov@alum.mit.edu
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package com.google.protobuf.gwt.server;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.gwt.shared.JsonStream;
import com.google.protobuf.gwt.shared.JsonStreamFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Factory class that uses "native" protobuf reflection functionality to create
 * a JsonStream object out of a native protobuf message. Once created, the
 * JsonStream object can be used to instantiate a GWT- friendly version of the
 * protobuf message.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public class ServerJsonStreamFromProtoFactory {
  protected static ServerJsonStreamFromProtoFactory instance;

  public static ServerJsonStreamFromProtoFactory getInstance() {
    if (instance == null) {
      instance = ServerJsonStreamFromProtoFactory
          .createFactory(ServerJsonStreamFactory.getInstance());
    }
    return instance;
  }

  protected final JsonStreamFactory jsonStreamFactory;

  protected ServerJsonStreamFromProtoFactory(
      JsonStreamFactory jsonStreamFactory) {
    this.jsonStreamFactory = jsonStreamFactory;
  }

  public static ServerJsonStreamFromProtoFactory createFactory(
      JsonStreamFactory jsonStreamFactory) {
    return jsonStreamFactory == null ? null :
        new ServerJsonStreamFromProtoFactory(jsonStreamFactory);
  }

  public JsonStreamFactory getJsonStreamFactory() {
    return this.jsonStreamFactory;
  }

  public JsonStream createFromMessage(com.google.protobuf.Message message)
      throws IOException {
    JsonStream stream = null;
    if (message != null) {
      stream = this.jsonStreamFactory.createNewStream();

      // For every field that is actually present
      Map<Descriptors.FieldDescriptor, Object> fields = message.getAllFields();
      for (Descriptors.FieldDescriptor fieldDescriptor : fields.keySet()) {
        // If the field is not an extension
        if (!fieldDescriptor.isExtension()) {
          if (fieldDescriptor.isOptional() || fieldDescriptor.isRequired()) {
            // Writes optional or required
            this.writeOptionalOrRequiredField(stream, message, fieldDescriptor);
          } else {
            if (fieldDescriptor.isRepeated()) {
              // Writes repeated
              this.writeRepeatedField(stream, message, fieldDescriptor);
            }
          }
        }
      }
    }
    return stream;
  }

  private void writeOptionalOrRequiredField(JsonStream stream,
                                            com.google.protobuf.Message message,
                                            Descriptors.FieldDescriptor
                                                fieldDescriptor)
      throws IOException {
    switch (fieldDescriptor.getJavaType()) {
      case BOOLEAN:
        this.writeBooleanField(stream, message, fieldDescriptor);
        break;
      case DOUBLE:
        this.writeDoubleField(stream, message, fieldDescriptor);
        break;
      case FLOAT:
        this.writeFloatField(stream, message, fieldDescriptor);
        break;
      case INT:
        this.writeIntegerField(stream, message, fieldDescriptor);
        break;
      case LONG:
        this.writeLongField(stream, message, fieldDescriptor);
        break;
      case STRING:
        this.writeStringField(stream, message, fieldDescriptor);
        break;
      case ENUM:
        this.writeEnumField(stream, message, fieldDescriptor);
        break;
      case MESSAGE:
        this.writeMessageField(stream, message, fieldDescriptor);
        break;
    }
  }

  private void writeRepeatedField(JsonStream stream,
                                  com.google.protobuf.Message message,
                                  Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    switch (fieldDescriptor.getJavaType()) {
      case BOOLEAN:
        this.writeRepeatedBooleanField(stream, message, fieldDescriptor);
        break;
      case DOUBLE:
        this.writeRepeatedDoubleField(stream, message, fieldDescriptor);
        break;
      case FLOAT:
        this.writeRepeatedFloatField(stream, message, fieldDescriptor);
        break;
      case INT:
        this.writeRepeatedIntegerField(stream, message, fieldDescriptor);
        break;
      case LONG:
        this.writeRepeatedLongField(stream, message, fieldDescriptor);
        break;
      case STRING:
        this.writeRepeatedStringField(stream, message, fieldDescriptor);
        break;
      case ENUM:
        this.writeRepeatedEnumField(stream, message, fieldDescriptor);
        break;
      case MESSAGE:
        this.writeRepeatedMessageField(stream, message, fieldDescriptor);
        break;
    }
  }

  protected void writeIntegerField(JsonStream stream,
                                   com.google.protobuf.Message message,
                                   Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof Integer) {
      stream
          .writeInteger(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
              (Integer) fieldValue);
    }
  }

  protected void writeFloatField(JsonStream stream,
                                 com.google.protobuf.Message message,
                                 Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof Float) {
      stream.writeFloat(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
          (Float) fieldValue);
    }
  }

  protected void writeDoubleField(JsonStream stream,
                                  com.google.protobuf.Message message,
                                  Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof Double) {
      stream.writeDouble(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
          (Double) fieldValue);
    }
  }

  protected void writeLongField(JsonStream stream,
                                com.google.protobuf.Message message,
                                Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof Long) {
      stream.writeLong(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
          (Long) fieldValue);
    }
  }

  protected void writeBooleanField(JsonStream stream,
                                   com.google.protobuf.Message message,
                                   Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof Boolean) {
      stream
          .writeBoolean(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
              (Boolean) fieldValue);
    }
  }

  protected void writeStringField(JsonStream stream,
                                  com.google.protobuf.Message message,
                                  Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof String) {
      stream.writeString(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
          (String) fieldValue);
    }
  }

  protected void writeEnumField(JsonStream stream,
                                com.google.protobuf.Message message,
                                Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof EnumValueDescriptor) {
      stream
          .writeInteger(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
              ((EnumValueDescriptor) fieldValue).getNumber());
    }
  }

  protected void writeMessageField(JsonStream stream,
                                   com.google.protobuf.Message message,
                                   Descriptors.FieldDescriptor fieldDescriptor)
      throws IOException {
    Object fieldValue = message.getField(fieldDescriptor);
    if (fieldValue instanceof com.google.protobuf.Message) {
      stream.writeStream(fieldDescriptor.getNumber(), fieldDescriptor.getName(),
          createFromMessage((com.google.protobuf.Message) fieldValue));
    }
  }

  protected void writeRepeatedIntegerField(JsonStream stream,
                                           com.google.protobuf.Message message,
                                           Descriptors.FieldDescriptor
                                               fieldDescriptor)
      throws IOException {
    List<Integer> repeatedFieldValueList = new ArrayList<Integer>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof Integer) {
        repeatedFieldValueList.add((Integer) repeatedFieldValue);
      }
    }
    stream.writeIntegerRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedFloatField(JsonStream stream,
                                         com.google.protobuf.Message message,
                                         Descriptors.FieldDescriptor
                                             fieldDescriptor)
      throws IOException {
    List<Float> repeatedFieldValueList = new ArrayList<Float>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof Float) {
        repeatedFieldValueList.add((Float) repeatedFieldValue);
      }
    }
    stream.writeFloatRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedDoubleField(JsonStream stream,
                                          com.google.protobuf.Message message,
                                          Descriptors.FieldDescriptor
                                              fieldDescriptor)
      throws IOException {
    List<Double> repeatedFieldValueList = new ArrayList<Double>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof Double) {
        repeatedFieldValueList.add((Double) repeatedFieldValue);
      }
    }
    stream.writeDoubleRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedLongField(JsonStream stream,
                                        com.google.protobuf.Message message,
                                        Descriptors.FieldDescriptor
                                            fieldDescriptor)
      throws IOException {
    List<Long> repeatedFieldValueList = new ArrayList<Long>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof Long) {
        repeatedFieldValueList.add((Long) repeatedFieldValue);
      }
    }
    stream.writeLongRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedBooleanField(JsonStream stream,
                                           com.google.protobuf.Message message,
                                           Descriptors.FieldDescriptor
                                               fieldDescriptor)
      throws IOException {
    List<Boolean> repeatedFieldValueList = new ArrayList<Boolean>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof Boolean) {
        repeatedFieldValueList.add((Boolean) repeatedFieldValue);
      }
    }
    stream.writeBooleanRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedStringField(JsonStream stream,
                                          com.google.protobuf.Message message,
                                          Descriptors.FieldDescriptor
                                              fieldDescriptor)
      throws IOException {
    List<String> repeatedFieldValueList = new ArrayList<String>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof String) {
        repeatedFieldValueList.add((String) repeatedFieldValue);
      }
    }
    stream.writeStringRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedEnumField(JsonStream stream,
                                        com.google.protobuf.Message message,
                                        Descriptors.FieldDescriptor
                                            fieldDescriptor)
      throws IOException {
    List<Integer> repeatedFieldValueList = new ArrayList<Integer>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof EnumValueDescriptor) {
        stream.writeInteger(fieldDescriptor.getNumber(),
            fieldDescriptor.getName(),
            ((EnumValueDescriptor) repeatedFieldValue).getNumber());
      }
    }
    stream.writeIntegerRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected void writeRepeatedMessageField(JsonStream stream,
                                           com.google.protobuf.Message message,
                                           Descriptors.FieldDescriptor
                                               fieldDescriptor)
      throws IOException {
    List<JsonStream> repeatedFieldValueList = new ArrayList<JsonStream>();
    int repeatedFieldCount = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < repeatedFieldCount; ++i) {
      Object repeatedFieldValue = message.getRepeatedField(fieldDescriptor, i);
      if (repeatedFieldValue instanceof com.google.protobuf.Message) {
        repeatedFieldValueList.add(this.createFromMessage(
            (com.google.protobuf.Message) repeatedFieldValue));
      }
    }
    stream.writeStreamRepeated(fieldDescriptor.getNumber(),
        getRepeatedFieldName(fieldDescriptor.getName()),
        repeatedFieldValueList);
  }

  protected static String getRepeatedFieldName(String fieldName) {
    if (fieldName != null) {
      return new StringBuffer(fieldName).append(" list").toString();
    }
    return null;
  }
}
