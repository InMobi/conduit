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
import com.google.protobuf.gwt.shared.JsonStream;
import com.google.protobuf.gwt.shared.JsonStreamFactory;

/**
 * A factory class that makes it easy to switch between different client-side
 * JsonStream implementations.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov
 */
public class ClientJsonStreamFactory extends JsonStreamFactory {
  protected static ClientJsonStreamFactory instance;

  public static ClientJsonStreamFactory getInstance() {
    if (instance == null) {
      instance =
          ClientJsonStreamFactory.createFactory(DEFAULT_IMPLEMENTATION_TYPE);
    }
    return instance;
  }

  protected ClientJsonStreamFactory(ImplementationType implementationType) {
    super(implementationType);
  }

  public static ClientJsonStreamFactory createFactory(
      ImplementationType implementationType) {
    return implementationType == null ? null :
        new ClientJsonStreamFactory(implementationType);
  }

  public JsonStream createNewStream(ImplementationType implementationType) {
    if (implementationType != null) {
      return implementationType.equals(ImplementationType.VERBOSE) ?
          new VerboseGWTJsonStream() : new CompactGWTJsonStream();
    }
    return null;
  }

  public GWTJsonStream createNewStreamFromJson(String jsonText) {
    JSONObject jsonObject = GWTJsonStream.parseJSONObject(jsonText);
    if (jsonObject != null) {
      // Figures out the type of JSON implementation that generated the text
      ImplementationType implementationType =
          this.getImplementationType(jsonObject);
      return implementationType.equals(ImplementationType.VERBOSE) ?
          new VerboseGWTJsonStream(jsonObject) :
          new CompactGWTJsonStream(jsonObject);
    }
    return null;
  }

  protected ImplementationType getImplementationType(JSONObject jsonObject) {
    if (jsonObject != null) {
      String jsonEncoding = GWTJsonStream
          .jsonValueToString(jsonObject.get(JSON_ENCODING_PARAMETER_KEY));
      if (jsonEncoding != null && jsonEncoding
          .equals(VERBOSE_JSON_STREAM_IMPLEMENTATION_PARAMETER_VALUE)) {
        return ImplementationType.VERBOSE;
      }
    }
    return ImplementationType.COMPACT;
  }
}
