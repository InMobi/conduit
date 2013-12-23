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

import java.io.IOException;

/**
 * Factory interface that makes it easy to switch between different json strema
 * implementations both on the client and on the server side.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public abstract class JsonStreamFactory {
  // Two different JSON implementations are supported at the moment at the
  // client and the server: "compact" - the more efficient of the two, and
  // "verbose" - an easier to read version better for debugging.
  public static enum ImplementationType {
    COMPACT,
    VERBOSE
  }

  // If you change this default, make sure that all the server/client-side
  // clients are re-built to know about your changes!!!
  protected static final ImplementationType DEFAULT_IMPLEMENTATION_TYPE =
      ImplementationType.COMPACT;

  public static final String JSON_ENCODING_PARAMETER_KEY = "json-encoding";

  public static final String
      VERBOSE_JSON_STREAM_IMPLEMENTATION_PARAMETER_VALUE = "verbose";
  public static final String
      COMPACT_JSON_STREAM_IMPLEMENTATION_PARAMETER_VALUE = "compact";

  protected final ImplementationType implementationType;

  protected JsonStreamFactory(ImplementationType implementationType) {
    this.implementationType = implementationType;
  }

  public abstract JsonStream createNewStreamFromJson(String jsonText);

  public abstract JsonStream createNewStream(
      ImplementationType implementationType);

  public ImplementationType getImplementationType() {
    return this.implementationType;
  }

  public JsonStream createNewStream() {
    return this.createNewStream(this.implementationType);
  }

  public JsonStream createNewStreamFromMessage(Message message) {
    return this.createNewStreamFromMessage(message, this.implementationType);
  }

  public JsonStream createNewStreamFromMessage(Message message,
                                               ImplementationType
                                                   implementationType) {
    JsonStream messageStream = null;
    if (message != null && implementationType != null) {
      try {
        message
            .writeTo(messageStream = this.createNewStream(implementationType));
      } catch (IOException e) {
      }
    }
    return messageStream;
  }

  public String serializeMessage(Message message) {
    return this.serializeMessage(message, this.implementationType);
  }

  public String serializeMessage(Message message,
                                 ImplementationType implementationType) {
    return this.serializeMessage(message, implementationType, false);
  }

  public String serializeMessage(Message message,
                                 ImplementationType implementationType,
                                 boolean prettyPrint) {
    JsonStream stream =
        this.createNewStreamFromMessage(message, implementationType);
    if (stream != null) {
      return stream.toJsonString(prettyPrint);
    }
    return null;
  }

  public boolean equals(Object object) {
    if (object != null) {
      if (object instanceof JsonStreamFactory) {
        JsonStreamFactory that = (JsonStreamFactory) object;
        if (that != null) {
          return this.implementationType.equals(that.implementationType);
        }
      }
    }
    return false;
  }
}
