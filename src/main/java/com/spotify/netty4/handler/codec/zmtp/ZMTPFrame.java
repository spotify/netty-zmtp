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

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;

import static io.netty.util.CharsetUtil.UTF_8;


public class ZMTPFrame implements ReferenceCounted {

  public static final ZMTPFrame EMPTY_FRAME = new ZMTPFrame(Unpooled.EMPTY_BUFFER);

  private final ByteBuf content;

  ZMTPFrame(final ByteBuf content) {
    this.content = content;
  }

  /**
   * @return Is the current frame empty
   */
  public boolean hasContent() {
    return content.isReadable();
  }

  /**
   * Get the frame contents.
   */
  public ByteBuf content() {
    return content;
  }

  /**
   * Returns the length of the data
   */
  public int size() {
    return content.readableBytes();
  }

  /**
   * Create a frame from a string, UTF-8 encoded.
   */
  public static ZMTPFrame from(final String data) {
    return from(data, UTF_8);
  }

  /**
   * Create a new frame from a string
   *
   * @param data    String
   * @param charset Used to get the bytes
   * @return a ZMTP frame containing the byte encoded string
   */
  public static ZMTPFrame from(final String data, final Charset charset) {
    if (data.length() == 0) {
      return EMPTY_FRAME;
    } else {
      return from(Unpooled.wrappedBuffer(charset.encode(data)));
    }
  }

  /**
   * Create a new frame from a byte array.
   */
  public static ZMTPFrame from(final byte[] data) {
    if (data == null || data.length == 0) {
      return EMPTY_FRAME;
    } else {
      return new ZMTPFrame(Unpooled.wrappedBuffer(data));
    }
  }

  /**
   * Create a new frame from a buffer. Claims ownership of the buffer.
   */
  public static ZMTPFrame from(final ByteBuf buf) {
    if (!buf.isReadable()) {
      return EMPTY_FRAME;
    } else {
      return new ZMTPFrame(buf);
    }
  }

  @Override
  public int refCnt() {
    return content.refCnt();
  }

  @Override
  public ReferenceCounted retain() {
    return content.retain();
  }

  @Override
  public ReferenceCounted retain(final int increment) {
    return content.retain(increment);
  }

  @Override
  public boolean release() {
    return content.release();
  }

  @Override
  public boolean release(final int decrement) {
    return content.release(decrement);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPFrame zmtpFrame = (ZMTPFrame) o;

    if (content != null ? !content.equals(zmtpFrame.content) : zmtpFrame.content != null) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    return content != null ? content.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "ZMTPFrame{\"" +
           ZMTPUtils.toString(content) +
           "\"}";
  }
}
