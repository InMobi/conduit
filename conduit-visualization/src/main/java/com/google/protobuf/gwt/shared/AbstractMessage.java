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

import java.util.Collection;

/**
 * A partial implementation of the {@link Message} interface which implements as
 * many methods of that interface as possible in terms of other methods.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov Based on the original non-GWT
 *         Java implementation by kenton@google.com Kenton Varda
 */
public abstract class AbstractMessage implements Message {
  /**
   * A partial implementation of the {@link Message.Builder} interface which
   * implements as many methods of that interface as possible in terms of other
   * methods.
   */
  public static abstract class Builder<BuilderType extends Builder>
      implements Message.Builder {
    // The compiler produces an error if this is not declared explicitly.
    @Override
    public abstract BuilderType clone();

    /**
     * Construct an UninitializedMessageException reporting missing fields in
     * the given message.
     */
    protected static UninitializedMessageException
    newUninitializedMessageException(
        Message message) {
      return new UninitializedMessageException(message);
    }

    /**
     * Adds the {@code values} to the {@code list}.  This is a helper method
     * used by generated code.  Users should ignore it.
     *
     * @throws NullPointerException if any of the elements of {@code values} is
     *                              null.
     */
    protected static <T> void addAll(final Iterable<T> values,
                                     final Collection<? super T> list) {
      for (final T value : values) {
        if (value == null) {
          throw new NullPointerException();
        }
      }
      if (values instanceof Collection) {
        final Collection<T> collection = (Collection<T>) values;
        list.addAll(collection);
      } else {
        for (final T value : values) {
          list.add(value);
        }
      }
    }
  }
}
