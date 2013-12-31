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

import java.io.IOException;

/**
 * Thrown when a protocol message being parsed is invalid in some way.
 *
 * @author vkulikov@alum.mit.edu Vitaliy Kulikov Based on the original non-GWT
 *         Java implementation by kenton@google.com Kenton Varda
 */
@SuppressWarnings("serial")
public class InvalidProtocolBufferException extends IOException {
  public InvalidProtocolBufferException(final String description) {
    super(description);
  }

  public static InvalidProtocolBufferException failedToReadField(
      String fieldKey) {
    return new InvalidProtocolBufferException(
        "Failed to read field " + fieldKey);
  }

  //
  // Integer:
  //

  public static InvalidProtocolBufferException failedToReadInteger(
      String fieldKey) {
    return failedToReadType(fieldKey, "integer");
  }

  public static InvalidProtocolBufferException failedToReadIntegerRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "integer");
  }

  //
  // Float:
  //

  public static InvalidProtocolBufferException failedToReadFloat(
      String fieldKey) {
    return failedToReadType(fieldKey, "float");
  }

  public static InvalidProtocolBufferException failedToReadFloatRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "float");
  }

  //
  // Double:
  //

  public static InvalidProtocolBufferException failedToReadDouble(
      String fieldKey) {
    return failedToReadType(fieldKey, "double");
  }

  public static InvalidProtocolBufferException failedToReadDoubleRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "double");
  }

  //
  // Long:
  //

  public static InvalidProtocolBufferException failedToReadLong(
      String fieldKey) {
    return failedToReadType(fieldKey, "long");
  }

  public static InvalidProtocolBufferException failedToReadLongRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "long");
  }

  //
  // Boolean:
  //

  public static InvalidProtocolBufferException failedToReadBoolean(
      String fieldKey) {
    return failedToReadType(fieldKey, "boolean");
  }

  public static InvalidProtocolBufferException failedToReadBooleanRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "boolean");
  }

  //
  // String:
  //

  public static InvalidProtocolBufferException failedToReadString(
      String fieldKey) {
    return failedToReadType(fieldKey, "string");
  }

  public static InvalidProtocolBufferException failedToReadStringRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "string");
  }

  //
  // JsonObject:
  //

  public static InvalidProtocolBufferException failedToReadObject(
      String fieldKey) {
    return failedToReadType(fieldKey, "object");
  }

  public static InvalidProtocolBufferException failedToReadObjectRepeated(
      String fieldKey) {
    return failedToReadTypeRepeated(fieldKey, "object");
  }

  protected static InvalidProtocolBufferException failedToReadType(
      String fieldKey, String type) {
    return new InvalidProtocolBufferException(
        "Failed to read field " + fieldKey + ": " + type + " expected");
  }

  protected static InvalidProtocolBufferException failedToReadTypeRepeated(
      String fieldKey, String type) {
    return new InvalidProtocolBufferException(
        "Failed to read field " + fieldKey + ": repeated " + type +
            " expected");
  }

	/*
  public static InvalidProtocolBufferException failedToReadField(
			int fieldNumber) {
		return
			new InvalidProtocolBufferException("Failed to read field " + fieldNumber);
	}
	
	//
	// Integer:
	//
	
	public static InvalidProtocolBufferException failedToReadInteger(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "integer");
	}
	
	public static InvalidProtocolBufferException failedToReadIntegerRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "integer");
	}
	
	//
	// Float:
	//
	
	public static InvalidProtocolBufferException failedToReadFloat(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "float");
	}
	
	public static InvalidProtocolBufferException failedToReadFloatRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "float");
	}
	
	//
	// Double:
	//
	
	public static InvalidProtocolBufferException failedToReadDouble(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "double");
	}
	
	public static InvalidProtocolBufferException failedToReadDoubleRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "double");
	}
	
	//
	// Long:
	//
	
	public static InvalidProtocolBufferException failedToReadLong(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "long");
	}
	
	public static InvalidProtocolBufferException failedToReadLongRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "long");
	}
	
	//
	// Boolean:
	//
	
	public static InvalidProtocolBufferException failedToReadBoolean(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "boolean");
	}
	
	public static InvalidProtocolBufferException failedToReadBooleanRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "boolean");
	}
	
	//
	// String:
	//
	
	public static InvalidProtocolBufferException failedToReadString(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "string");
	}
	
	public static InvalidProtocolBufferException failedToReadStringRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "string");
	}
	
	//
	// JsonObject:
	//
	
	public static InvalidProtocolBufferException failedToReadObject(int
	fieldNumber) {
		return failedToReadType(fieldNumber, "object");
	}
	
	public static InvalidProtocolBufferException failedToReadObjectRepeated(int
	fieldNumber) {
		return failedToReadTypeRepeated(fieldNumber, "object");
	}
	
	protected static InvalidProtocolBufferException failedToReadType(
			int fieldNumber, String type) {
		return
			new InvalidProtocolBufferException(
					"Failed to read field " + fieldNumber + ": " + type + " expected");
	}
	
	protected static InvalidProtocolBufferException failedToReadTypeRepeated(
			int fieldNumber, String type) {
		return
			new InvalidProtocolBufferException(
					"Failed to read field " + fieldNumber + ": repeated " + type + "
					expected");
	}*/
}
