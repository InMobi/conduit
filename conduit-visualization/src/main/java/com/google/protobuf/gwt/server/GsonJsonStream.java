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

import com.google.gson.*;
import com.google.protobuf.gwt.shared.AbstractJsonStream;
import com.google.protobuf.gwt.shared.InvalidProtocolBufferException;
import com.google.protobuf.gwt.shared.JsonStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Simple class that implements useful utility methods on top of the Gson JSON
 * implementation.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public abstract class GsonJsonStream extends AbstractJsonStream {
  protected final JsonObject json;

  protected GsonJsonStream(JsonObject json) {
    this.json = json;
  }

  protected JsonObject getJsonObject() {
    return this.json;
  }

  protected abstract GsonJsonStream newStream(JsonObject jsonObject);

  //
  // Utility methods, also useful when parsing third-party JSON
  //
  public static JsonObject parseJsonObject(String jsonText) {
    if (jsonText != null) {
      try {
        JsonElement jsonValue = new JsonParser().parse(jsonText);
        if (jsonValue != null && jsonValue.isJsonObject()) {
          return jsonValue.getAsJsonObject();
        }
      } catch (Exception e) {
      }
    }
    return null;
  }

  public static JsonArray parseJsonArray(String jsonText) {
    if (jsonText != null) {
      try {
        JsonElement jsonValue = new JsonParser().parse(jsonText);
        if (jsonValue != null && jsonValue.isJsonArray()) {
          return jsonValue.getAsJsonArray();
        }
      } catch (Exception e) {
      }
    }
    return null;
  }

  public static Integer jsonElementToInteger(JsonElement json) {
    if (json != null && json.isJsonPrimitive()) {
      JsonPrimitive jsonPrimitive = json.getAsJsonPrimitive();
      if (jsonPrimitive.isNumber()) {
        return jsonPrimitive.getAsInt();
      }
    }
    return null;
  }

  public static Boolean jsonElementToBoolean(JsonElement jsonElement) {
    if (jsonElement != null && jsonElement.isJsonPrimitive()) {
      JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
      if (jsonPrimitive.isBoolean()) {
        return jsonPrimitive.getAsBoolean();
      }
    }
    return null;
  }

  public static Float jsonElementToFloat(JsonElement jsonElement) {
    if (jsonElement != null && jsonElement.isJsonPrimitive()) {
      JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
      if (jsonPrimitive.isNumber()) {
        return jsonPrimitive.getAsFloat();
      }
    }
    return null;
  }

  public static Double jsonElementToDouble(JsonElement jsonElement) {
    if (jsonElement != null && jsonElement.isJsonPrimitive()) {
      JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
      if (jsonPrimitive.isNumber()) {
        return jsonPrimitive.getAsDouble();
      }
    }
    return null;
  }

  public static Long jsonElementToLong(JsonElement jsonElement) {
    if (jsonElement != null && jsonElement.isJsonPrimitive()) {
      JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
      if (jsonPrimitive.isNumber()) {
        return jsonPrimitive.getAsLong();
      }
    }
    return null;
  }

  public static String jsonElementToString(JsonElement json) {
    if (json != null && json.isJsonPrimitive()) {
      JsonPrimitive jsonPrimitive = json.getAsJsonPrimitive();
      if (jsonPrimitive.isString()) {
        return jsonPrimitive.getAsString();
      }
    }
    return null;
  }

  public static JsonArray jsonElementToArray(JsonElement jsonElement) {
    if (jsonElement != null && jsonElement.isJsonArray()) {
      return jsonElement.getAsJsonArray();
    }
    return null;
  }

  public static JsonObject jsonElementToObject(JsonElement jsonElement) {
    if (jsonElement != null && jsonElement.isJsonObject()) {
      return jsonElement.getAsJsonObject();
    }
    return null;
  }

  //
  // Integer:
  //

  protected Integer readInteger(JsonObject jsonObject, String fieldLabel,
                                int fieldNumber)
      throws InvalidProtocolBufferException {
    Integer fieldInteger = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        fieldInteger = jsonElementToInteger(fieldElement);
        if (fieldInteger == null) {
          throw InvalidProtocolBufferException.failedToReadInteger(fieldLabel);
        }
      }
    }
    return fieldInteger;
  }

  protected List<Integer> readIntegerRepeated(JsonObject jsonObject,
                                              String fieldLabel,
                                              int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Integer> fieldIntegerRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldIntegerRepeated = new ArrayList<Integer>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            Integer fieldInteger = jsonElementToInteger(fieldJsonArray.get(i));
            if (fieldInteger != null) {
              fieldIntegerRepeated.add(fieldInteger);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadIntegerRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadIntegerRepeated(fieldLabel);
        }
      }
    }
    return fieldIntegerRepeated;
  }

  protected JsonObject writeInteger(JsonObject jsonObject, String fieldLabel,
                                    int fieldInteger) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.add(fieldLabel, new JsonPrimitive(fieldInteger));
    }
    return jsonObject;
  }

  protected JsonObject writeIntegerRepeated(JsonObject jsonObject,
                                            String fieldLabel,
                                            Collection<Integer>
                                                fieldIntegerRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldIntegerRepeated != null && !fieldIntegerRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (Integer fieldInteger : fieldIntegerRepeated) {
        fieldJsonArray.add(new JsonPrimitive(fieldInteger.intValue()));
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  //
  // Float:
  //

  protected Float readFloat(JsonObject jsonObject, String fieldLabel,
                            int fieldNumber)
      throws InvalidProtocolBufferException {
    Float fieldFloat = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        fieldFloat = jsonElementToFloat(fieldElement);
        if (fieldFloat == null) {
          throw InvalidProtocolBufferException.failedToReadFloat(fieldLabel);
        }
      }
    }
    return fieldFloat;
  }

  protected List<Float> readFloatRepeated(JsonObject jsonObject,
                                          String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Float> fieldFloatRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldFloatRepeated = new ArrayList<Float>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            Float fieldFloat = jsonElementToFloat(fieldJsonArray.get(i));
            if (fieldFloat != null) {
              fieldFloatRepeated.add(fieldFloat);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadFloatRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadFloatRepeated(fieldLabel);
        }
      }
    }
    return fieldFloatRepeated;
  }

  protected JsonObject writeFloat(JsonObject jsonObject, String fieldLabel,
                                  float fieldFloat) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.add(fieldLabel, new JsonPrimitive(fieldFloat));
    }
    return jsonObject;
  }

  protected JsonObject writeFloatRepeated(JsonObject jsonObject,
                                          String fieldLabel,
                                          Collection<Float>
                                              fieldFloatRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldFloatRepeated != null && !fieldFloatRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (Float fieldFloat : fieldFloatRepeated) {
        fieldJsonArray.add(new JsonPrimitive(fieldFloat.floatValue()));
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  //
  // Double:
  //

  protected Double readDouble(JsonObject jsonObject, String fieldLabel,
                              int fieldNumber)
      throws InvalidProtocolBufferException {
    Double fieldDouble = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        fieldDouble = jsonElementToDouble(fieldElement);
        if (fieldDouble == null) {
          throw InvalidProtocolBufferException.failedToReadDouble(fieldLabel);
        }
      }
    }
    return fieldDouble;
  }

  protected List<Double> readDoubleRepeated(JsonObject jsonObject,
                                            String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Double> fieldDoubleRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldDoubleRepeated = new ArrayList<Double>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            Double fieldDouble = jsonElementToDouble(fieldJsonArray.get(i));
            if (fieldDouble != null) {
              fieldDoubleRepeated.add(fieldDouble);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadDoubleRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadDoubleRepeated(fieldLabel);
        }
      }
    }
    return fieldDoubleRepeated;
  }

  protected JsonObject writeDouble(JsonObject jsonObject, String fieldLabel,
                                   double fieldDouble) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.add(fieldLabel, new JsonPrimitive(fieldDouble));
    }
    return jsonObject;
  }

  protected JsonObject writeDoubleRepeated(JsonObject jsonObject,
                                           String fieldLabel,
                                           Collection<Double>
                                               fieldDoubleRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldDoubleRepeated != null && !fieldDoubleRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (Double fieldDouble : fieldDoubleRepeated) {
        fieldJsonArray.add(new JsonPrimitive(fieldDouble.doubleValue()));
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  //
  // Long:
  //

  protected Long readLong(JsonObject jsonObject, String fieldLabel,
                          int fieldNumber)
      throws InvalidProtocolBufferException {
    Long fieldLong = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        fieldLong = jsonElementToLong(fieldElement);
        if (fieldLong == null) {
          throw InvalidProtocolBufferException.failedToReadLong(fieldLabel);
        }
      }
    }
    return fieldLong;
  }

  protected List<Long> readLongRepeated(JsonObject jsonObject,
                                        String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Long> fieldLongRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldLongRepeated = new ArrayList<Long>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            Long fieldLong = jsonElementToLong(fieldJsonArray.get(i));
            if (fieldLong != null) {
              fieldLongRepeated.add(fieldLong);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadLongRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadLongRepeated(fieldLabel);
        }
      }
    }
    return fieldLongRepeated;
  }

  protected JsonObject writeLong(JsonObject jsonObject, String fieldLabel,
                                 long fieldLong) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.add(fieldLabel, new JsonPrimitive(fieldLong));
    }
    return jsonObject;
  }

  protected JsonObject writeLongRepeated(JsonObject jsonObject,
                                         String fieldLabel,
                                         Collection<Long> fieldLongRepeated) {
    if (jsonObject != null && fieldLabel != null && fieldLongRepeated != null &&
        !fieldLongRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (Long fieldLong : fieldLongRepeated) {
        fieldJsonArray.add(new JsonPrimitive(fieldLong.longValue()));
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  //
  // Boolean:
  //

  protected Boolean readBoolean(JsonObject jsonObject, String fieldLabel,
                                int fieldNumber)
      throws InvalidProtocolBufferException {
    Boolean fieldBoolean = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        fieldBoolean = jsonElementToBoolean(fieldElement);
        if (fieldBoolean == null) {
          throw InvalidProtocolBufferException.failedToReadBoolean(fieldLabel);
        }
      }
    }
    return fieldBoolean;
  }

  protected List<Boolean> readBooleanRepeated(JsonObject jsonObject,
                                              String fieldLabel,
                                              int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Boolean> fieldBooleanRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldBooleanRepeated = new ArrayList<Boolean>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            Boolean fieldBoolean = jsonElementToBoolean(fieldJsonArray.get(i));
            if (fieldBoolean != null) {
              fieldBooleanRepeated.add(fieldBoolean);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadBooleanRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadBooleanRepeated(fieldLabel);
        }
      }
    }
    return fieldBooleanRepeated;
  }

  protected JsonObject writeBoolean(JsonObject jsonObject, String fieldLabel,
                                    boolean fieldBoolean) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.add(fieldLabel, new JsonPrimitive(fieldBoolean));
    }
    return jsonObject;
  }

  protected JsonObject writeBooleanRepeated(JsonObject jsonObject,
                                            String fieldLabel,
                                            Collection<Boolean>
                                                fieldBooleanRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldBooleanRepeated != null && !fieldBooleanRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (Boolean fieldBoolean : fieldBooleanRepeated) {
        fieldJsonArray.add(new JsonPrimitive(fieldBoolean.booleanValue()));
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  //
  // String:
  //

  protected String readString(JsonObject jsonObject, String fieldLabel,
                              int fieldNumber)
      throws InvalidProtocolBufferException {
    String fieldString = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        fieldString = jsonElementToString(fieldElement);
        if (fieldString == null) {
          throw InvalidProtocolBufferException.failedToReadString(fieldLabel);
        }
      }
    }
    return fieldString;
  }

  protected List<String> readStringRepeated(JsonObject jsonObject,
                                            String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<String> fieldStringRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldStringRepeated = new ArrayList<String>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            String fieldString = jsonElementToString(fieldJsonArray.get(i));
            if (fieldString != null) {
              fieldStringRepeated.add(fieldString);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadStringRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadStringRepeated(fieldLabel);
        }
      }
    }
    return fieldStringRepeated;
  }

  protected JsonObject writeString(JsonObject jsonObject, String fieldLabel,
                                   String fieldString) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.add(fieldLabel, new JsonPrimitive(fieldString));
    }
    return jsonObject;
  }

  protected JsonObject writeStringRepeated(JsonObject jsonObject,
                                           String fieldLabel,
                                           Collection<String>
                                               fieldStringRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldStringRepeated != null && !fieldStringRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (String fieldString : fieldStringRepeated) {
        fieldJsonArray.add(new JsonPrimitive(fieldString));
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  //
  // JsonStream:
  //

  public JsonStream readStream(JsonObject jsonObject, String fieldLabel,
                               int fieldNumber)
      throws InvalidProtocolBufferException {
    JsonStream fieldStream = null;
    if (jsonObject != null && fieldLabel != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonObject fieldJsonObject = jsonElementToObject(fieldElement);
        if (fieldJsonObject != null) {
          fieldStream = this.newStream(fieldJsonObject);
        }
        if (fieldStream == null) {
          throw InvalidProtocolBufferException.failedToReadObject(fieldLabel);
        }
      }
    }
    return fieldStream;
  }

  public List<JsonStream> readStreamRepeated(JsonObject jsonObject,
                                             String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<JsonStream> fieldStreamRepeated = null;
    if (jsonObject != null) {
      JsonElement fieldElement = jsonObject.get(fieldLabel);
      if (fieldElement != null) {
        JsonArray fieldJsonArray = jsonElementToArray(fieldElement);
        if (fieldJsonArray != null) {
          fieldStreamRepeated = new ArrayList<JsonStream>();
          for (int i = 0; i < fieldJsonArray.size(); ++i) {
            JsonStream fieldStream = null;
            JsonObject fieldJsonObject =
                jsonElementToObject(fieldJsonArray.get(i));
            if (fieldJsonObject != null) {
              fieldStream = this.newStream(fieldJsonObject);
            }
            if (fieldStream != null) {
              fieldStreamRepeated.add(fieldStream);
            } else {
              throw InvalidProtocolBufferException
                  .failedToReadObjectRepeated(fieldLabel);
            }
          }
        } else {
          throw InvalidProtocolBufferException
              .failedToReadObjectRepeated(fieldLabel);
        }
      }
    }
    return fieldStreamRepeated;
  }

  protected JsonObject writeStream(JsonObject jsonObject, String fieldLabel,
                                   JsonStream fieldStream) throws IOException {
    if (jsonObject != null && fieldLabel != null) {
      if (fieldStream instanceof GsonJsonStream) {
        GsonJsonStream fieldGsonJsonStream = (GsonJsonStream) fieldStream;
        jsonObject.add(fieldLabel, fieldGsonJsonStream.getJsonObject());
      } else {
        throw new IOException("Failed to write stream field " + fieldLabel);
      }
    }
    return jsonObject;
  }

  protected JsonObject writeStreamRepeated(JsonObject jsonObject,
                                           String fieldLabel,
                                           Collection<JsonStream>
                                               fieldStreamRepeated)
      throws IOException {
    if (jsonObject != null && fieldLabel != null &&
        fieldStreamRepeated != null && !fieldStreamRepeated.isEmpty()) {
      JsonArray fieldJsonArray = new JsonArray();
      for (JsonStream fieldStream : fieldStreamRepeated) {
        if (fieldStream instanceof GsonJsonStream) {
          GsonJsonStream fieldGsonJsonStream = (GsonJsonStream) fieldStream;
          fieldJsonArray.add(fieldGsonJsonStream.getJsonObject());
        } else {
          throw new IOException(
              "Failed to write repeated stream field " + fieldLabel);
        }
      }
      jsonObject.add(fieldLabel, fieldJsonArray);
    }
    return jsonObject;
  }

  public String toJsonString() {
    return this.toJsonString(false);
  }

  public String toJsonString(boolean pretty) {
    // TODO: implement pretty version!!!
    return this.json.toString();
  }

  public boolean equals(Object object) {
    if (object != null) {
      if (object instanceof GsonJsonStream) {
        GsonJsonStream that = (GsonJsonStream) object;
        return this.json.equals(that.json);
      }
    }
    return false;
  }

  public int hashCode() {
    return this.json.hashCode();
  }

  public String toString() {
    return this.json.toString();
  }
}
