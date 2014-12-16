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

package com.spotify.netty4.handler.codec.zmtp;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;


public class ZMTPFrame {

  public static final ZMTPFrame EMPTY_FRAME = create();

  private final byte[] data;

  ZMTPFrame(final byte[] data) {
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
   * Return the channel buffer container the frame data.
   */
  public ByteBuf data() {
    if (data == null) {
      return EMPTY_BUFFER;
    } else {
      return Unpooled.wrappedBuffer(data);
    }
  }

  /**
   * Returns the length of the data
   */
  public int size() {
    return data == null ? 0 : data.length;
  }

  /**
   * Create a frame from a string
   *
   * @return a frame containing the string as default byte encoding
   */
  static public ZMTPFrame create(final String data) {
    return wrap(data.getBytes());
  }

  /**
   * Create a new frame from a string
   *
   * @param data        String
   * @param charsetName Used to get the bytes
   * @return a ZMTP frame containing the byte encoded string
   */
  static public ZMTPFrame create(final String data, final String charsetName)
      throws UnsupportedEncodingException {
    return create(data, Charset.forName(charsetName));
  }

  /**
   * Create a new frame from a string
   *
   * @param data    String
   * @param charset Used to get the bytes
   * @return a ZMTP frame containing the byte encoded string
   */
  public static ZMTPFrame create(final String data, final Charset charset) {
    if (data.length() == 0) {
      return EMPTY_FRAME;
    } else {
      return wrap(data.getBytes(charset));
    }
  }

  /**
   * Create a new frame from a byte array. The byte array will not be copied.
   */
  public static ZMTPFrame wrap(final byte[] data) {
    if (data == null || data.length == 0) {
      return EMPTY_FRAME;
    } else {
      return new ZMTPFrame(data);
    }
  }

  /**
   * Create a new frame from a buffer. The buffer is not retained.
   */
  public static ZMTPFrame copy(final ByteBuf buf) {
    if (!buf.isReadable()) {
      return EMPTY_FRAME;
    } else {
      final byte[] data = new byte[buf.readableBytes()];
      buf.slice().readBytes(data);
      return new ZMTPFrame(data);
    }
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

  @Override
  public String toString() {
    return "ZMTPFrame{\"" +
           ZMTPUtils.toString(data()) +
           "\"}";
  }

  public static ZMTPFrame create() {
    return new ZMTPFrame(null);
  }
}
