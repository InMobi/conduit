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

package com.google.protobuf.gwt.shared;

import java.util.Collection;
import java.util.List;

/**
 * Interface used by the generated GWT-friendly protobuf Java code to serialize
 * protobufs to and from JSON. Provided that you use different JSON
 * implementations on the client and server, you will need to implement this
 * interface twice.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public interface JsonStream {

  public JsonStream newStream();

  //
  // Integer:
  //

  public Integer readInteger(int fieldNumber) throws java.io.IOException;

  public List<Integer> readIntegerRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeInteger(int fieldNumber, String fieldName, int value)
      throws java.io.IOException;

  public JsonStream writeIntegerRepeated(int fieldNumber, String fieldName,
                                         Collection<Integer> values)
      throws java.io.IOException;

  //
  // Float:
  //

  public Float readFloat(int fieldNumber) throws java.io.IOException;

  public List<Float> readFloatRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeFloat(int fieldNumber, String fieldName, float value)
      throws java.io.IOException;

  public JsonStream writeFloatRepeated(int fieldNumber, String fieldName,
                                       Collection<Float> values)
      throws java.io.IOException;

  //
  // Double:
  //

  public Double readDouble(int fieldNumber) throws java.io.IOException;

  public List<Double> readDoubleRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeDouble(int fieldNumber, String fieldName, double value)
      throws java.io.IOException;

  public JsonStream writeDoubleRepeated(int fieldNumber, String fieldName,
                                        Collection<Double> values)
      throws java.io.IOException;

  //
  // Long:
  //

  public Long readLong(int fieldNumber) throws java.io.IOException;

  public List<Long> readLongRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeLong(int fieldNumber, String fieldName, long value)
      throws java.io.IOException;

  public JsonStream writeLongRepeated(int fieldNumber, String fieldName,
                                      Collection<Long> values)
      throws java.io.IOException;

  //
  // Boolean:
  //

  public Boolean readBoolean(int fieldNumber) throws java.io.IOException;

  public List<Boolean> readBooleanRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeBoolean(int fieldNumber, String fieldName,
                                 boolean value) throws java.io.IOException;

  public JsonStream writeBooleanRepeated(int fieldNumber, String fieldName,
                                         Collection<Boolean> values)
      throws java.io.IOException;

  //
  // String:
  //

  public String readString(int fieldNumber) throws java.io.IOException;

  public List<String> readStringRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeString(int fieldNumber, String fieldName, String value)
      throws java.io.IOException;

  public JsonStream writeStringRepeated(int fieldNumber, String fieldName,
                                        Collection<String> values)
      throws java.io.IOException;

  //
  // JsonStream:
  //

  public JsonStream readStream(int fieldNumber) throws java.io.IOException;

  public List<JsonStream> readStreamRepeated(int fieldNumber)
      throws java.io.IOException;

  public JsonStream writeStream(int fieldNumber, String fieldName,
                                JsonStream value) throws java.io.IOException;

  public JsonStream writeStreamRepeated(int fieldNumber, String fieldName,
                                        Collection<JsonStream> values)
      throws java.io.IOException;

  //
  // Message:
  //

  public void writeMessage(int fieldNumber, String fieldName, Message message)
      throws java.io.IOException;

  public void writeMessageRepeated(int fieldNumber, String fieldName,
                                   List<? extends Message> messageList)
      throws java.io.IOException;

  public String toJsonString();

  public String toJsonString(boolean pretty);
}