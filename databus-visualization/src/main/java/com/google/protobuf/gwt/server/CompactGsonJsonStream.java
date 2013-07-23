package com.google.protobuf.gwt.server;

import com.google.gson.JsonObject;
import com.google.protobuf.gwt.shared.InvalidProtocolBufferException;
import com.google.protobuf.gwt.shared.JsonStream;
import com.google.protobuf.gwt.shared.Message;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CompactGsonJsonStream extends GsonJsonStream {

  protected CompactGsonJsonStream(JsonObject json) {
    super(json);
  }

  public CompactGsonJsonStream() {
    this(new JsonObject());
  }

  public static CompactGsonJsonStream createStreamFromMessage(Message message)
      throws java.io.IOException {
    CompactGsonJsonStream messageStream = null;
    if (message != null) {
      message.writeTo(messageStream = new CompactGsonJsonStream());
    }
    return messageStream;
  }

  public GsonJsonStream newStream() {
    return new CompactGsonJsonStream();
  }

  public GsonJsonStream newStream(JsonObject jsonObject) {
    return jsonObject == null ? null : new CompactGsonJsonStream(jsonObject);
  }

  //
  // Integer:
  //

  public Integer readInteger(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this.readInteger(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<Integer> readIntegerRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readIntegerRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeInteger(int fieldNumber, String fieldLabel,
                                 int fieldInteger) {
    this.writeInteger(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldInteger);
    return this;
  }

  public JsonStream writeIntegerRepeated(int fieldNumber, String fieldLabel,
                                         Collection<Integer>
                                             fieldIntegerRepeated) {
    this.writeIntegerRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldIntegerRepeated);
    return this;
  }

  //
  // Float:
  //

  public Float readFloat(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this.readFloat(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<Float> readFloatRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readFloatRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeFloat(int fieldNumber, String fieldLabel,
                               float fieldFloat) {
    this.writeFloat(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldFloat);
    return this;
  }

  public JsonStream writeFloatRepeated(int fieldNumber, String fieldLabel,
                                       Collection<Float> fieldFloatRepeated) {
    this.writeFloatRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldFloatRepeated);
    return this;
  }

  //
  // Double:
  //

  public Double readDouble(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this.readDouble(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<Double> readDoubleRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readDoubleRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeDouble(int fieldNumber, String fieldLabel,
                                double fieldDouble) {
    this.writeDouble(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldDouble);
    return this;
  }

  public JsonStream writeDoubleRepeated(int fieldNumber, String fieldLabel,
                                        Collection<Double>
                                            fieldDoubleRepeated) {
    this.writeDoubleRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldDoubleRepeated);
    return this;
  }

  //
  // Long:
  //

  public Long readLong(int fieldNumber) throws InvalidProtocolBufferException {
    return this.readLong(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<Long> readLongRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readLongRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeLong(int fieldNumber, String fieldLabel,
                              long fieldLong) {
    this.writeLong(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldLong);
    return this;
  }

  public JsonStream writeLongRepeated(int fieldNumber, String fieldLabel,
                                      Collection<Long> fieldLongRepeated) {
    this.writeLongRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldLongRepeated);
    return this;
  }

  //
  // Boolean:
  //

  public Boolean readBoolean(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this.readBoolean(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<Boolean> readBooleanRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readBooleanRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeBoolean(int fieldNumber, String fieldLabel,
                                 boolean fieldBoolean) {
    this.writeBoolean(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldBoolean);
    return this;
  }

  public JsonStream writeBooleanRepeated(int fieldNumber, String fieldLabel,
                                         Collection<Boolean>
                                             fieldBooleanRepeated) {
    this.writeBooleanRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldBooleanRepeated);
    return this;
  }

  //
  // String:
  //

  public String readString(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this.readString(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<String> readStringRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readStringRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeString(int fieldNumber, String fieldLabel,
                                String fieldString) {
    this.writeString(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldString);
    return this;
  }

  public JsonStream writeStringRepeated(int fieldNumber, String fieldLabel,
                                        Collection<String>
                                            fieldStringRepeated) {
    this.writeStringRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldStringRepeated);
    return this;
  }

  //
  // JsonStream:
  //

  public JsonStream readStream(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this.readStream(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldNumber);
  }

  public List<JsonStream> readStreamRepeated(int fieldNumber)
      throws InvalidProtocolBufferException {
    return this
        .readStreamRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
            fieldNumber);
  }

  public JsonStream writeStream(int fieldNumber, String fieldLabel,
                                JsonStream fieldStream) throws IOException {
    this.writeStream(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldStream);
    return this;
  }

  public JsonStream writeStreamRepeated(int fieldNumber, String fieldLabel,
                                        Collection<JsonStream> fieldStreamRepeated)
      throws IOException {
    this.writeStreamRepeated(this.json, this.getKeyForFieldNumber(fieldNumber),
        fieldStreamRepeated);
    return this;
  }
}
