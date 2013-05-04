/*
 * Copyright (c) 2012-2013 Spotify AB
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
package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class ZMTPFrame {

  private final byte[] data;

  private ZMTPFrame(final byte[] data) {
    this.data = data;
  }

  /**
   * @return Is the current frame empty
   */
  public boolean hasData() {
    // Empty frame only contains flag byte
    return data != null;
  }

  /**
   * Returns the data for a frame
   */
  public byte[] getData() {
    return data;
  }

  /**
   * Returns the length of the data
   */
  public int size() {
    return (data == null ? 0 : data.length);
  }

  /**
   * Create a frame from a string
   *
   * @return a frame containing the string as default byte encoding
   */
  static public ZMTPFrame create(final String data) {
    return create(data.getBytes());
  }

  /**
   * Create a new frame from a string
   *
   * @param data    String
   * @param charset Used to get the bytes
   * @return a ZMTP frame containing the byte encoded string
   */
  static public ZMTPFrame create(final String data, final String charset)
      throws UnsupportedEncodingException {
    return create(data.getBytes(charset));
  }

  /**
   * Returns a new frame based on the byte array
   */
  static public ZMTPFrame create(final byte[] data) {
    return new ZMTPFrame(data);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ZMTPFrame zmtpFrame = (ZMTPFrame) o;

    if (!Arrays.equals(data, zmtpFrame.data)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return data != null ? Arrays.hashCode(data) : 0;
  }

  /**
   * Helper used during decoding of a netty frame
   *
   * @param length length of buffer
   */
  static public ZMTPFrame read(final ChannelBuffer buffer, final int length) {
    byte[] data = null;

    if (length > 0) {
      data = new byte[length];

      buffer.readBytes(data);
    }

    return new ZMTPFrame(data);
  }

  @Override
  public String toString() {
    return "ZMTPFrame{" +
           ", data=" + ZMTPUtils.toString(data) +
           '}';
  }

  public static ZMTPFrame create() {
    return new ZMTPFrame(null);
  }
}
