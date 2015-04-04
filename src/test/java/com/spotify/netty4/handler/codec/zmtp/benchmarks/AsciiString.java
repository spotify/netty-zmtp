/*
 * Copyright (c) 2012-2014 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.netty4.handler.codec.zmtp.benchmarks;

import com.google.common.base.Function;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;

import static com.google.common.base.Preconditions.checkNotNull;

public class AsciiString implements CharSequence {

  public static final Function<String, AsciiString>
      ASCII_STRING_FROM_STRING =
      new Function<String, AsciiString>() {
        @Override
        public AsciiString apply(final String input) {
          return from(input);
        }
      };

  private final byte[] chars;

  public AsciiString(final byte[] chars) {
    this.chars = checkNotNull(chars, "chars");
  }

  @Override
  public int length() {
    return chars.length;
  }

  @Override
  public char charAt(final int index) {
    return (char) chars[index];
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    final byte[] chars = new byte[end - start];
    System.arraycopy(this.chars, start, chars, 0, end - start);
    return new AsciiString(chars);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final AsciiString that = (AsciiString) o;

    return Arrays.equals(chars, that.chars);
  }

  @Override
  public int hashCode() {
    return chars != null ? Arrays.hashCode(chars) : 0;
  }

  @Override
  public String toString() {
    final char[] chars = new char[this.chars.length];
    for (int i = 0; i < this.chars.length; i++) {
      chars[i] = (char) this.chars[i];
    }
    return new String(chars);
  }

  public static AsciiString from(final String s) {
    final byte[] chars = new byte[s.length()];
    for (int i = 0; i < s.length(); i++) {
      chars[i] = (byte) s.charAt(i);
    }
    return new AsciiString(chars);
  }

  public void write(final ByteBuf buf) {
    buf.writeBytes(chars);
  }
}
