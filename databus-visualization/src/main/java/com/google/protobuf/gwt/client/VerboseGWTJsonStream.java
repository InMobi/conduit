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

package com.google.protobuf.gwt.client;

import com.google.gwt.json.client.JSONObject;
import com.google.gwt.json.client.JSONValue;
import com.google.protobuf.gwt.shared.InvalidProtocolBufferException;
import com.google.protobuf.gwt.shared.JsonStream;
import com.google.protobuf.gwt.shared.JsonStreamFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Client-side verbose implementation of the {@link JsonStream} interface, using
 * GWT JSON implementation. In addition to field numbers, this implementation
 * includes information such as field names. It is less efficient, and therefore
 * should not be used in production unless performance is not your biggest
 * concern.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public class VerboseGWTJsonStream extends GWTJsonStream {

  protected VerboseGWTJsonStream(JSONObject json) {
    super(json);
  }

  public VerboseGWTJsonStream() {
    this(new JSONObject());
  }

  public GWTJsonStream newStream() {
    return new VerboseGWTJsonStream();
  }

  public GWTJsonStream newStream(JSONObject jsonObject) {
    return jsonObject == null ? null : new VerboseGWTJsonStream(jsonObject);
  }

  //
  // Integer:
  //

  public Integer readInteger(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readInteger(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<Integer> readIntegerRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readIntegerRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeInteger(int fieldNumber, String fieldLabel,
                                 int fieldInteger) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeIntegerField(fieldNumber, fieldLabel, fieldInteger));
  }

  public JsonStream writeIntegerRepeated(int fieldNumber, String fieldLabel,
                                         Collection<Integer>
                                             fieldIntegerRepeated) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeIntegerRepeatedField(fieldNumber, fieldLabel,
            fieldIntegerRepeated));
  }

  protected JSONObject writeIntegerField(int fieldNumber, String fieldLabel,
                                         int fieldInteger) {
    return this
        .writeInteger(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldInteger);
  }

  protected JSONObject writeIntegerRepeatedField(int fieldNumber,
                                                 String fieldLabel,
                                                 Collection<Integer>
                                                     fieldIntegerRepeated) {
    return this.writeIntegerRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldIntegerRepeated);
  }

  //
  // Float:
  //

  public Float readFloat(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readFloat(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<Float> readFloatRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readFloatRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeFloat(int fieldNumber, String fieldLabel,
                               float fieldFloat) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeFloatField(fieldNumber, fieldLabel, fieldFloat));
  }

  public JsonStream writeFloatRepeated(int fieldNumber, String fieldLabel,
                                       Collection<Float> fieldFloatRepeated) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeFloatRepeatedField(fieldNumber, fieldLabel,
            fieldFloatRepeated));
  }

  protected JSONObject writeFloatField(int fieldNumber, String fieldLabel,
                                       float fieldFloat) {
    return this
        .writeFloat(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldFloat);
  }

  protected JSONObject writeFloatRepeatedField(int fieldNumber,
                                               String fieldLabel,
                                               Collection<Float>
                                                   fieldFloatRepeated) {
    return this.writeFloatRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldFloatRepeated);
  }

  //
  // Double:
  //

  public Double readDouble(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readDouble(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<Double> readDoubleRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readDoubleRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeDouble(int fieldNumber, String fieldLabel,
                                double fieldDouble) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeDoubleField(fieldNumber, fieldLabel, fieldDouble));
  }

  public JsonStream writeDoubleRepeated(int fieldNumber, String fieldLabel,
                                        Collection<Double>
                                            fieldDoubleRepeated) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeDoubleRepeatedField(fieldNumber, fieldLabel,
            fieldDoubleRepeated));
  }

  protected JSONObject writeDoubleField(int fieldNumber, String fieldLabel,
                                        double fieldDouble) {
    return this
        .writeDouble(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldDouble);
  }

  protected JSONObject writeDoubleRepeatedField(int fieldNumber,
                                                String fieldLabel,
                                                Collection<Double>
                                                    fieldDoubleRepeated) {
    return this.writeDoubleRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldDoubleRepeated);
  }

  //
  // Long:
  //

  public Long readLong(int fieldNumber) throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readLong(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<Long> readLongRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readLongRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeLong(int fieldNumber, String fieldLabel,
                              long fieldLong) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeLongField(fieldNumber, fieldLabel, fieldLong));
  }

  public JsonStream writeLongRepeated(int fieldNumber, String fieldLabel,
                                      Collection<Long> fieldLongRepeated) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeLongRepeatedField(fieldNumber, fieldLabel,
            fieldLongRepeated));
  }

  protected JSONObject writeLongField(int fieldNumber, String fieldLabel,
                                      long fieldLong) {
    return this
        .writeLong(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldLong);
  }

  protected JSONObject writeLongRepeatedField(int fieldNumber,
                                              String fieldLabel,
                                              Collection<Long>
                                                  fieldLongRepeated) {
    return this.writeLongRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldLongRepeated);
  }

  //
  // Boolean:
  //

  public Boolean readBoolean(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readBoolean(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<Boolean> readBooleanRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readBooleanRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeBoolean(int fieldNumber, String fieldLabel,
                                 boolean fieldBoolean) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeBooleanField(fieldNumber, fieldLabel, fieldBoolean));
  }

  public JsonStream writeBooleanRepeated(int fieldNumber, String fieldLabel,
                                         Collection<Boolean>
                                             fieldBooleanRepeated) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeBooleanRepeatedField(fieldNumber, fieldLabel,
            fieldBooleanRepeated));
  }

  protected JSONObject writeBooleanField(int fieldNumber, String fieldLabel,
                                         boolean fieldBoolean) {
    return this
        .writeBoolean(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldBoolean);
  }

  protected JSONObject writeBooleanRepeatedField(int fieldNumber,
                                                 String fieldLabel,
                                                 Collection<Boolean>
                                                     fieldBooleanRepeated) {
    return this.writeBooleanRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldBooleanRepeated);
  }

  //
  // String:
  //

  public String readString(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readString(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<String> readStringRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readStringRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeString(int fieldNumber, String fieldLabel,
                                String fieldString) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeStringField(fieldNumber, fieldLabel, fieldString));
  }

  public JsonStream writeStringRepeated(int fieldNumber, String fieldLabel,
                                        Collection<String>
                                            fieldStringRepeated) {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeStringRepeatedField(fieldNumber, fieldLabel,
            fieldStringRepeated));
  }

  protected JSONObject writeStringField(int fieldNumber, String fieldLabel,
                                        String fieldString) {
    return this
        .writeString(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldString);
  }

  protected JSONObject writeStringRepeatedField(int fieldNumber,
                                                String fieldLabel,
                                                Collection<String>
                                                    fieldStringRepeated) {
    return this.writeStringRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldStringRepeated);
  }

  //
  // JsonStream:
  //

  public JsonStream readStream(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readStream(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public List<JsonStream> readStreamRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    JSONObject fieldJsonObject = this.readFieldJsonObject(fieldNumber);
    return fieldJsonObject == null ? null :
        this.readStreamRepeated(fieldJsonObject, FIELD_VALUE_KEY, fieldNumber);
  }

  public JsonStream writeStream(int fieldNumber, String fieldLabel,
                                JsonStream fieldStream) throws IOException {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeStreamField(fieldNumber, fieldLabel, fieldStream));
  }

  public JsonStream writeStreamRepeated(int fieldNumber, String fieldLabel,
                                        Collection<JsonStream>
                                            fieldStreamRepeated)
      throws IOException {
    return this.writeFieldJsonObject(fieldNumber, fieldLabel,
        this.writeStreamRepeatedField(fieldNumber, fieldLabel,
            fieldStreamRepeated));
  }

  protected JSONObject writeStreamField(int fieldNumber, String fieldLabel,
                                        JsonStream fieldStream)
      throws IOException {
    return this
        .writeStream(this.createLabelledObject(fieldLabel), FIELD_VALUE_KEY,
            fieldStream);
  }

  protected JSONObject writeStreamRepeatedField(int fieldNumber,
                                                String fieldLabel,
                                                Collection<JsonStream>
                                                    fieldStreamRepeated)
      throws IOException {
    return this.writeStreamRepeated(this.createLabelledObject(fieldLabel),
        FIELD_VALUE_KEY, fieldStreamRepeated);
  }

  protected JSONObject createLabelledObject(String fieldLabel) {
    JSONObject fieldJsonObject = null;
    if (fieldLabel != null) {
      fieldJsonObject = new JSONObject();
      this.writeString(fieldJsonObject, FIELD_LABEL_KEY, fieldLabel);
    }
    return fieldJsonObject;
  }

  protected JSONObject readFieldJsonObject(int fieldNumber)
      throws InvalidProtocolBufferException {
    String fieldKey = this.getKeyForFieldNumber(fieldNumber);
    if (fieldKey != null) {
      JSONValue fieldJsonValue = this.json.get(fieldKey);
      if (fieldJsonValue != null) {
        JSONObject fieldJSONObject = fieldJsonValue.isObject();
        if (fieldJSONObject != null) {
          return fieldJSONObject;
        } else {
          throw InvalidProtocolBufferException.failedToReadField(fieldKey);
        }
      }
    }
    return null;
  }

  protected JsonStream writeFieldJsonObject(int fieldNumber, String fieldLabel,
                                            JSONObject fieldJsonObject) {
    if (fieldLabel != null && fieldJsonObject != null) {
      String fieldKey = this.getKeyForFieldNumber(fieldNumber);
      if (fieldKey != null) {
        this.json.put(fieldKey, fieldJsonObject);
      }
    }
    return this;
  }

  public String toJsonString(boolean pretty) {
    // Writes a JSON_ENCODING_PARAMETER_KEY flag to indicate that this is "verbose"
    // implementation.
    // Implementation is assumed to be "compact" by default.
    this.writeString(this.json,
        ClientJsonStreamFactory.JSON_ENCODING_PARAMETER_KEY,
        JsonStreamFactory.VERBOSE_JSON_STREAM_IMPLEMENTATION_PARAMETER_VALUE);
    return !pretty ? this.json.toString() :
        this.jsonNonPrimitiveToPrettyString(this.json, 0);
  }
}
