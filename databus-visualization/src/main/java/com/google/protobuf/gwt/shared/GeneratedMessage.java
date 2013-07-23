// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// http://code.google.com/p/protobuf/
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

/**
 * Gwt version of {@link GeneratedMessage}.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov Based on the original non-GWT
 *         Java implementation by kenton@google.com Kenton Varda
 */
public abstract class GeneratedMessage extends AbstractMessage {
  protected GeneratedMessage() {
  }

  public abstract static class Builder<MessageType extends GeneratedMessage,
      BuilderType extends Builder>
      extends AbstractMessage.Builder<BuilderType> {
    protected Builder() {
    }

    // This is implemented here only to work around an apparent bug in the
    // Java compiler and/or build system.  See bug #1898463.  The mere presence
    // of this dummy clone() implementation makes it go away.
    @Override
    public BuilderType clone() {
      throw new UnsupportedOperationException(
          "This is supposed to be overridden by subclasses.");
    }

    /**
     * All subclasses implement this.
     */
    public abstract BuilderType mergeFrom(MessageType message);

    /**
     * All subclasses implement this.
     */
    public abstract BuilderType readFrom(JsonStream input)
        throws java.io.IOException;

    // Defined here for return type covariance.
    public abstract MessageType getDefaultInstanceForType();

    /**
     * Get the message being built.  We don't just pass this to the constructor
     * because it becomes null when build() is called.
     */
    protected abstract MessageType internalGetResult();
  }
}
