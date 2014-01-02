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

import java.util.ArrayList;
import java.util.List;

/**
 * A partial implementation of the {@link JsonStream} interface, which is
 * expected to be shared between the client and server.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public abstract class AbstractJsonStream implements JsonStream {
  // Field keys used by verbose stream implementations
  protected static final String FIELD_LABEL_KEY = "label";
  protected static final String FIELD_VALUE_KEY = "value";

  public void writeMessage(int fieldNumber, String fieldName, Message message)
      throws java.io.IOException {
    if (message != null && 0 < fieldNumber) {
      JsonStream messageStream = this.newStream();
      message.writeTo(messageStream);
      this.writeStream(fieldNumber, fieldName, messageStream);
    }
  }

  public void writeMessageRepeated(int fieldNumber, String fieldName,
                                   List<? extends Message> messageList)
      throws java.io.IOException {
    if (messageList != null && 0 < fieldNumber) {
      List<JsonStream> messageStreamList = new ArrayList<JsonStream>();
      for (Message message : messageList) {
        JsonStream messageStream = this.newStream();
        message.writeTo(messageStream);
        messageStreamList.add(messageStream);
      }
      this.writeStreamRepeated(fieldNumber, fieldName, messageStreamList);
    }
  }

  protected String getFieldLabelKey() {
    return FIELD_LABEL_KEY;
  }

  protected String getFieldValueKey() {
    return FIELD_VALUE_KEY;
  }

  protected String getKeyForFieldNumber(int fieldNumber) {
    // Having a number as a key string is OK in JSON!
    return fieldNumber <= 0 ? null : Integer.toString(fieldNumber);
  }
}
