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

// TODO(kenton):  Use generics?  E.g. Builder<BuilderType extends Builder>, then
//   mergeFrom*() could return BuilderType for better type-safety.

package com.google.protobuf.gwt.shared;

/**
 * Abstract interface implemented by Protocol Message GWT objects.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov Based on the original non-GWT
 *         Java implementation by kenton@google.com Kenton Varda
 */
public interface Message {
  /**
   * Get an instance of the type with all fields set to their default values.
   * This may or may not be a singleton.  This differs from the {@code
   * getDefaultInstance()} method of generated message classes in that this
   * method is an abstract method of the {@code MessageLite} interface whereas
   * {@code getDefaultInstance()} is a static method of a specific class.  They
   * return the same thing.
   */
  Message getDefaultInstanceForType();

  /**
   * Returns true if all required fields in the message and all embedded
   * messages are set, false otherwise.
   */
  boolean isInitialized();

  /**
   * Serializes the message and writes it to {@code output}.
   */
  public void writeTo(JsonStream output) throws java.io.IOException;

  // =================================================================
  // Builders

  /**
   * Constructs a new builder for a message of the same type as this message.
   */
  Builder newBuilderForType();

  /**
   * Constructs a builder initialized with the current message.  Use this to
   * derive a new message from the current one.
   */
  Builder toBuilder();

  /**
   * Abstract interface implemented by Protocol Message builders.
   */
  interface Builder extends Cloneable {
    /**
     * Resets all fields to their default values.
     */
    Builder clear();

    /**
     * Merge {@code other} into the message being built.  {@code other} must
     * have the exact same type as {@code this}.
     * <p/>
     * Merging occurs as follows.  For each field:<br> * For singular primitive
     * fields, if the field is set in {@code other}, then {@code other}'s value
     * overwrites the value in this message.<br> * For singular message fields,
     * if the field is set in {@code other}, it is merged into the corresponding
     * sub-message of this message using the same merging rules.<br> * For
     * repeated fields, the elements in {@code other} are concatenated with the
     * elements in this message.
     * <p/>
     * This is equivalent to the {@code Message::MergeFrom} method in C++.
     */
    Builder mergeFrom(Message other);

    /**
     * Merge {@code input} into the message being built.
     */
    Builder readFrom(JsonStream input) throws java.io.IOException;

    /**
     * Construct the final message.  Once this is called, the Builder is no
     * longer valid, and calling any other method will result in undefined
     * behavior and may throw a NullPointerException.  If you need to continue
     * working with the builder after calling {@code build()}, {@code clone()}
     * it first.
     *
     * @throws UninitializedMessageException The message is missing one or more
     *                                       required fields (i.e. {@link
     *                                       #isInitialized()} returns false) .
     *                                       Use {@link #buildPartial()} to
     *                                       bypass this check.
     */
    Message build();

    /**
     * Like {@link #build()}, but does not throw an exception if the message is
     * missing required fields.  Instead, a partial message is returned. Once
     * this is called, the Builder is no longer valid, and calling any will
     * result in undefined behavior and may throw a NullPointerException.
     * <p/>
     * If you need to continue working with the builder after calling {@code
     * buildPartial()}, {@code clone()} it first.
     */
    Message buildPartial();

    /**
     * Clones the Builder.
     *
     * @see Object#clone()
     */
    Builder clone();

    /**
     * Returns true if all required fields in the message and all embedded
     * messages are set, false otherwise.
     */
    boolean isInitialized();

    /**
     * Get the message's type's default instance. See {@link
     * Message#getDefaultInstanceForType()}.
     */
    Message getDefaultInstanceForType();
  }
}
