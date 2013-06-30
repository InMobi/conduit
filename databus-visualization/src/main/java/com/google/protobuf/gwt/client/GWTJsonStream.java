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

import com.google.gwt.json.client.*;
import com.google.protobuf.gwt.shared.AbstractJsonStream;
import com.google.protobuf.gwt.shared.InvalidProtocolBufferException;
import com.google.protobuf.gwt.shared.JsonStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Simple class that implements useful utility methods on top of the GWT JSON
 * implementation.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public abstract class GWTJsonStream extends AbstractJsonStream {
  protected final JSONObject json;

  protected GWTJsonStream(JSONObject json) {
    this.json = json;
  }

  protected JSONObject getJSONObject() {
    return this.json;
  }

  protected abstract GWTJsonStream newStream(JSONObject jsonObject);

  //
  // Utility methods, also useful when parsing third-party JSON
  //
  public static JSONObject parseJSONObject(String jsonText) {
    if (jsonText != null && 0 < jsonText.length()) {
      // TODO: use GWT json utils class to do safe parsing
      JSONValue jsonValue = JSONParser.parse(jsonText);
      if (jsonValue != null) {
        return jsonValue.isObject();
      }
    }
    return null;
  }

  public static JSONArray parseJSONArray(String jsonText) {
    if (jsonText != null && 0 < jsonText.length()) {
      JSONValue jsonValue = JSONParser.parse(jsonText);
      if (jsonValue != null) {
        return jsonValue.isArray();
      }
    }
    return null;
  }

  public static JSONObject jsonValueToObject(JSONValue jsonValue) {
    if (jsonValue != null) {
      return jsonValue.isObject();
    }
    return null;
  }

  public static JSONArray jsonValueToArray(JSONValue jsonValue) {
    if (jsonValue != null) {
      return jsonValue.isArray();
    }
    return null;
  }

  public static Boolean jsonValueToBoolean(JSONValue jsonValue) {
    if (jsonValue != null) {
      JSONBoolean jsonBoolean = jsonValue.isBoolean();
      if (jsonBoolean != null) {
        return jsonBoolean.booleanValue();
      }
    }
    return null;
  }

  public static Float jsonValueToFloat(JSONValue jsonValue) {
    if (jsonValue != null) {
      JSONNumber jsonNumber = jsonValue.isNumber();
      if (jsonNumber != null) {
        return (float) jsonNumber.doubleValue();
      }
    }
    return null;
  }

  public static Double jsonValueToDouble(JSONValue jsonValue) {
    if (jsonValue != null) {
      JSONNumber jsonNumber = jsonValue.isNumber();
      if (jsonNumber != null) {
        return jsonNumber.doubleValue();
      }
    }
    return null;
  }

  public static Long jsonValueToLong(JSONValue jsonValue) {
    if (jsonValue != null) {
      JSONNumber jsonNumber = jsonValue.isNumber();
      if (jsonNumber != null) {
        return (long) jsonNumber.doubleValue();
      }
    }
    return null;
  }

  public static Integer jsonValueToInteger(JSONValue jsonValue) {
    if (jsonValue != null) {
      JSONNumber jsonNumber = jsonValue.isNumber();
      if (jsonNumber != null) {
        return (int) jsonNumber.doubleValue();
      }
    }
    return null;
  }

  public static String jsonValueToString(JSONValue jsonValue) {
    if (jsonValue != null) {
      JSONString jsonString = jsonValue.isString();
      if (jsonString == null) {
        return null;
      } else {
        return jsonString.stringValue();
      }
    }
    return null;
  }

  //
  // Integer:
  //

  protected Integer readInteger(JSONObject jsonObject, String fieldLabel,
                                int fieldNumber)
      throws InvalidProtocolBufferException {
    Integer fieldInteger = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        fieldInteger = jsonValueToInteger(fieldValue);
        if (fieldInteger == null) {
          throw InvalidProtocolBufferException.failedToReadInteger(fieldLabel);
        }
      }
    }
    return fieldInteger;
  }

  protected List<Integer> readIntegerRepeated(JSONObject jsonObject,
                                              String fieldLabel,
                                              int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Integer> fieldIntegerRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldIntegerRepeated = new ArrayList<Integer>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            Integer fieldInteger = jsonValueToInteger(fieldJSONArray.get(i));
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

  protected JSONObject writeInteger(JSONObject jsonObject, String fieldLabel,
                                    int fieldInteger) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.put(fieldLabel, new JSONNumber(fieldInteger));
    }
    return jsonObject;
  }

  protected JSONObject writeIntegerRepeated(JSONObject jsonObject,
                                            String fieldLabel,
                                            Collection<Integer>
                                                fieldIntegerRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldIntegerRepeated != null && !fieldIntegerRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (Integer fieldInteger : fieldIntegerRepeated) {
        fieldJSONArray.set(i++, new JSONNumber(fieldInteger.intValue()));
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  //
  // Float:
  //

  protected Float readFloat(JSONObject jsonObject, String fieldLabel,
                            int fieldNumber)
      throws InvalidProtocolBufferException {
    Float fieldFloat = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        fieldFloat = jsonValueToFloat(fieldValue);
        if (fieldFloat == null) {
          throw InvalidProtocolBufferException.failedToReadFloat(fieldLabel);
        }
      }
    }
    return fieldFloat;
  }

  protected List<Float> readFloatRepeated(JSONObject jsonObject,
                                          String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Float> fieldFloatRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldFloatRepeated = new ArrayList<Float>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            Float fieldFloat = jsonValueToFloat(fieldJSONArray.get(i));
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

  protected JSONObject writeFloat(JSONObject jsonObject, String fieldLabel,
                                  float fieldFloat) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.put(fieldLabel, new JSONNumber(fieldFloat));
    }
    return jsonObject;
  }

  protected JSONObject writeFloatRepeated(JSONObject jsonObject,
                                          String fieldLabel,
                                          Collection<Float>
                                              fieldFloatRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldFloatRepeated != null && !fieldFloatRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (Float fieldFloat : fieldFloatRepeated) {
        fieldJSONArray.set(i++, new JSONNumber(fieldFloat.floatValue()));
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  //
  // Double:
  //

  protected Double readDouble(JSONObject jsonObject, String fieldLabel,
                              int fieldNumber)
      throws InvalidProtocolBufferException {
    Double fieldDouble = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        fieldDouble = jsonValueToDouble(fieldValue);
        if (fieldDouble == null) {
          throw InvalidProtocolBufferException.failedToReadDouble(fieldLabel);
        }
      }
    }
    return fieldDouble;
  }

  protected List<Double> readDoubleRepeated(JSONObject jsonObject,
                                            String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Double> fieldDoubleRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldDoubleRepeated = new ArrayList<Double>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            Double fieldDouble = jsonValueToDouble(fieldJSONArray.get(i));
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

  protected JSONObject writeDouble(JSONObject jsonObject, String fieldLabel,
                                   double fieldDouble) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.put(fieldLabel, new JSONNumber(fieldDouble));
    }
    return jsonObject;
  }

  protected JSONObject writeDoubleRepeated(JSONObject jsonObject,
                                           String fieldLabel,
                                           Collection<Double>
                                               fieldDoubleRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldDoubleRepeated != null && !fieldDoubleRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (Double fieldDouble : fieldDoubleRepeated) {
        fieldJSONArray.set(i++, new JSONNumber(fieldDouble.doubleValue()));
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  //
  // Long:
  //

  protected Long readLong(JSONObject jsonObject, String fieldLabel,
                          int fieldNumber)
      throws InvalidProtocolBufferException {
    Long fieldLong = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        fieldLong = jsonValueToLong(fieldValue);
        if (fieldLong == null) {
          throw InvalidProtocolBufferException.failedToReadLong(fieldLabel);
        }
      }
    }
    return fieldLong;
  }

  protected List<Long> readLongRepeated(JSONObject jsonObject,
                                        String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Long> fieldLongRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldLongRepeated = new ArrayList<Long>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            Long fieldLong = jsonValueToLong(fieldJSONArray.get(i));
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

  protected JSONObject writeLong(JSONObject jsonObject, String fieldLabel,
                                 long fieldLong) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.put(fieldLabel, new JSONNumber(fieldLong));
    }
    return jsonObject;
  }

  protected JSONObject writeLongRepeated(JSONObject jsonObject,
                                         String fieldLabel,
                                         Collection<Long> fieldLongRepeated) {
    if (jsonObject != null && fieldLabel != null && fieldLongRepeated != null &&
        !fieldLongRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (Long fieldLong : fieldLongRepeated) {
        fieldJSONArray.set(i++, new JSONNumber(fieldLong.longValue()));
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  //
  // Boolean:
  //

  protected Boolean readBoolean(JSONObject jsonObject, String fieldLabel,
                                int fieldNumber)
      throws InvalidProtocolBufferException {
    Boolean fieldBoolean = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        fieldBoolean = jsonValueToBoolean(fieldValue);
        if (fieldBoolean == null) {
          throw InvalidProtocolBufferException.failedToReadBoolean(fieldLabel);
        }
      }
    }
    return fieldBoolean;
  }

  protected List<Boolean> readBooleanRepeated(JSONObject jsonObject,
                                              String fieldLabel,
                                              int fieldNumber)
      throws InvalidProtocolBufferException {
    List<Boolean> fieldBooleanRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldBooleanRepeated = new ArrayList<Boolean>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            Boolean fieldBoolean = jsonValueToBoolean(fieldJSONArray.get(i));
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

  protected JSONObject writeBoolean(JSONObject jsonObject, String fieldLabel,
                                    boolean fieldBoolean) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.put(fieldLabel, JSONBoolean.getInstance(fieldBoolean));
    }
    return jsonObject;
  }

  protected JSONObject writeBooleanRepeated(JSONObject jsonObject,
                                            String fieldLabel,
                                            Collection<Boolean>
                                                fieldBooleanRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldBooleanRepeated != null && !fieldBooleanRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (Boolean fieldBoolean : fieldBooleanRepeated) {
        fieldJSONArray
            .set(i++, JSONBoolean.getInstance(fieldBoolean.booleanValue()));
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  //
  // String:
  //

  protected String readString(JSONObject jsonObject, String fieldLabel,
                              int fieldNumber)
      throws InvalidProtocolBufferException {
    String fieldString = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        fieldString = jsonValueToString(fieldValue);
        if (fieldString == null) {
          throw InvalidProtocolBufferException.failedToReadString(fieldLabel);
        }
      }
    }
    return fieldString;
  }

  protected List<String> readStringRepeated(JSONObject jsonObject,
                                            String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<String> fieldStringRepeated = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldStringRepeated = new ArrayList<String>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            String fieldString = jsonValueToString(fieldJSONArray.get(i));
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

  protected JSONObject writeString(JSONObject jsonObject, String fieldLabel,
                                   String fieldString) {
    if (jsonObject != null && fieldLabel != null) {
      jsonObject.put(fieldLabel, new JSONString(fieldString));
    }
    return jsonObject;
  }

  protected JSONObject writeStringRepeated(JSONObject jsonObject,
                                           String fieldLabel,
                                           Collection<String>
                                               fieldStringRepeated) {
    if (jsonObject != null && fieldLabel != null &&
        fieldStringRepeated != null && !fieldStringRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (String fieldString : fieldStringRepeated) {
        fieldJSONArray.set(i++, new JSONString(fieldString));
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  //
  // JSONStream:
  //

  public JsonStream readStream(JSONObject jsonObject, String fieldLabel,
                               int fieldNumber)
      throws InvalidProtocolBufferException {
    JsonStream fieldStream = null;
    if (jsonObject != null && fieldLabel != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONObject fieldJSONObject = jsonValueToObject(fieldValue);
        if (fieldJSONObject != null) {
          fieldStream = this.newStream(fieldJSONObject);
        }
        if (fieldStream == null) {
          throw InvalidProtocolBufferException.failedToReadObject(fieldLabel);
        }
      }
    }
    return fieldStream;
  }

  public List<JsonStream> readStreamRepeated(JSONObject jsonObject,
                                             String fieldLabel, int fieldNumber)
      throws InvalidProtocolBufferException {
    List<JsonStream> fieldStreamRepeated = null;
    if (jsonObject != null) {
      JSONValue fieldValue = jsonObject.get(fieldLabel);
      if (fieldValue != null) {
        JSONArray fieldJSONArray = jsonValueToArray(fieldValue);
        if (fieldJSONArray != null) {
          fieldStreamRepeated = new ArrayList<JsonStream>();
          for (int i = 0; i < fieldJSONArray.size(); ++i) {
            JsonStream fieldStream = null;
            JSONObject fieldJSONObject =
                jsonValueToObject(fieldJSONArray.get(i));
            if (fieldJSONObject != null) {
              fieldStream = this.newStream(fieldJSONObject);
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

  protected JSONObject writeStream(JSONObject jsonObject, String fieldLabel,
                                   JsonStream fieldStream) throws IOException {
    if (jsonObject != null && fieldLabel != null) {
      if (fieldStream instanceof GWTJsonStream) {
        GWTJsonStream fieldGWTJsonStream = (GWTJsonStream) fieldStream;
        jsonObject.put(fieldLabel, fieldGWTJsonStream.getJSONObject());
      } else {
        throw new IOException("Failed to write stream field " + fieldLabel);
      }
    }
    return jsonObject;
  }

  protected JSONObject writeStreamRepeated(JSONObject jsonObject,
                                           String fieldLabel,
                                           Collection<JsonStream>
                                               fieldStreamRepeated)
      throws IOException {
    if (jsonObject != null && fieldLabel != null &&
        fieldStreamRepeated != null && !fieldStreamRepeated.isEmpty()) {
      JSONArray fieldJSONArray = new JSONArray();
      int i = 0;
      for (JsonStream fieldStream : fieldStreamRepeated) {
        if (fieldStream instanceof GWTJsonStream) {
          GWTJsonStream fieldGWTJsonStream = (GWTJsonStream) fieldStream;
          fieldJSONArray.set(i++, fieldGWTJsonStream.getJSONObject());
        } else {
          throw new IOException(
              "Failed to write repeated stream field " + fieldLabel);
        }
      }
      jsonObject.put(fieldLabel, fieldJSONArray);
    }
    return jsonObject;
  }

  public String toJsonString() {
    return this.toJsonString(false);
  }

  public String toJsonString(boolean pretty) {
    return !pretty ? this.json.toString() :
        this.jsonNonPrimitiveToPrettyString(this.json, 0);
  }

  // This method assumes that the JSON value being passed is a JSON
  // non-primitive:
  // either an object or an array.
  // This helps to make this implementation as efficient as possible.
  protected String jsonNonPrimitiveToPrettyString(JSONValue jsonValue,
                                                  int indentLevel) {
    StringBuffer buffer = new StringBuffer();
    // If the value in question is an object
    JSONObject jsonValueObject = jsonValue.isObject();
    if (jsonValueObject != null) {
      boolean firstKey = true;
      for (String key : jsonValueObject.keySet()) {
        if (firstKey) {
          firstKey = false;
        } else {
          buffer.append(",\n");
        }
        for (int k = 0; k < indentLevel; ++k) {
          buffer.append("  ");
        }
        buffer.append(key).append(" : ");
        JSONValue jsonObjectValue = jsonValueObject.get(key);
        if (jsonObjectValue != null) {
          if (jsonObjectValue.isObject() != null) {
            buffer.append("{\n").append(
                jsonNonPrimitiveToPrettyString(jsonObjectValue,
                    indentLevel + 1)).append("}");
          } else if (jsonObjectValue.isArray() != null) {
            buffer.append("[\n").append(
                jsonNonPrimitiveToPrettyString(jsonObjectValue,
                    indentLevel + 1)).append("]");
          } else {
            // If the object value is a primitive, just prints it,
            // there is no need for a recursive call!
            buffer.append(jsonObjectValue.toString());
          }
        }
      }
    } else if (jsonValue.isArray() != null) {
      // If the value in question is an array
      JSONArray jsonValueArray = jsonValue.isArray();
      for (int i = 0; i < jsonValueArray.size(); ++i) {
        if (0 < i) {
          buffer.append(",\n");
        }
        for (int k = 0; k < indentLevel; ++k) {
          buffer.append("  ");
        }
        JSONValue jsonArrayValue = jsonValueArray.get(i);
        if (jsonArrayValue != null) {
          if (jsonArrayValue.isObject() != null) {
            buffer.append("{\n").append(
                jsonNonPrimitiveToPrettyString(jsonArrayValue, indentLevel + 1))
                .append("}");
          } else if (jsonArrayValue.isArray() != null) {
            buffer.append("[\n").append(
                jsonNonPrimitiveToPrettyString(jsonArrayValue, indentLevel + 1))
                .append("]");
          } else {
            // If the object value is a primitive, just prints it,
            // there is no need for a recursive call!
            buffer.append(jsonArrayValue.toString());
          }
        }
      }
    }
    return buffer.toString();
  }

  public boolean equals(Object object) {
    if (object != null) {
      if (object instanceof GWTJsonStream) {
        GWTJsonStream that = (GWTJsonStream) object;
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
