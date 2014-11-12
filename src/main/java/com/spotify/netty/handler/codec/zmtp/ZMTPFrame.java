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
import java.nio.charset.Charset;

import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;
import static org.jboss.netty.buffer.ChannelBuffers.copiedBuffer;

public class ZMTPFrame {

  public static final ZMTPFrame EMPTY_FRAME = create();

  private final ChannelBuffer data;

  private ZMTPFrame(final ChannelBuffer data) {
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
   *
   * <p>Note: buffer contents and indices must not be modified.
   */
  public ChannelBuffer data() {
    if (data == null) {
      return EMPTY_BUFFER;
    } else {
      return data;
    }
  }

  /**
   * Returns the length of the data
   */
  public int size() {
    return data == null ? 0 : data.readableBytes();
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
      return create(copiedBuffer(data, charset));
    }
  }

  /**
   * Create a new frame from a byte array.
   */
  static public ZMTPFrame create(final byte[] data) {
    if (data == null || data.length == 0) {
      return EMPTY_FRAME;
    } else {
      return create(copiedBuffer(data));
    }
  }

  /**
   * Create a new frame from a channel buffer.
   */
  public static ZMTPFrame create(final ChannelBuffer buf) {
    if (!buf.readable()) {
      return EMPTY_FRAME;
    } else {
      return new ZMTPFrame(buf);
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

    if (data != null ? !data.equals(zmtpFrame.data) : zmtpFrame.data != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return data != null ? data.hashCode() : 0;
  }

  /**
   * Helper used during decoding of a ZMTP frame
   *
   * @param length length of buffer
   * @return A {@link ZMTPFrame} containg the data read from the buffer.
   */
  static public ZMTPFrame read(final ChannelBuffer buffer, final int length) {
    if (length > 0) {
      final ChannelBuffer data = buffer.readSlice(length);
      return new ZMTPFrame(data);
    } else {
      return EMPTY_FRAME;
    }
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
