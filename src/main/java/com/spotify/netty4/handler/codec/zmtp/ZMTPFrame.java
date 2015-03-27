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

  private final ByteBuf data;

  ZMTPFrame(final ByteBuf data) {
    this.data = data;
  }

  /**
   * @return Is the current frame empty
   */
  public boolean hasData() {
    return data.isReadable();
  }

  /**
   * Get the frame contents.
   */
  public ByteBuf data() {
    return data;
  }

  /**
   * Returns the length of the data
   */
  public int size() {
    return data.readableBytes();
  }

  /**
   * Create a frame by UTF-8 encoding a {@link CharSequence}.
   */
  public static ZMTPFrame fromUTF8(final CharSequence data) {
    return from(data, UTF_8);
  }

  /**
   * Create a frame by encoding a {@link CharSequence}.
   *
   * @param data    The {@link CharSequence} to encode.
   * @param charset The {@link Charset} to encode as.
   * @return a ZMTP frame containing the encoded {@link CharSequence}.
   */
  public static ZMTPFrame from(final CharSequence data, final Charset charset) {
    if (data.length() == 0) {
      return EMPTY_FRAME;
    } else {
      return from(Unpooled.copiedBuffer(data, charset));
    }
  }

  /**
   * Create a new frame from a byte array.
   */
  public static ZMTPFrame from(final byte[] data) {
    return new ZMTPFrame(Unpooled.wrappedBuffer(data));
  }

  /**
   * Create a new frame from a {@link ByteBuf}. Claims ownership of the buffer.
   */
  public static ZMTPFrame from(final ByteBuf buf) {
    return new ZMTPFrame(buf);
  }

  @Override
  public int refCnt() {
    return data.refCnt();
  }

  @Override
  public ReferenceCounted retain() {
    return data.retain();
  }

  @Override
  public ReferenceCounted retain(final int increment) {
    return data.retain(increment);
  }

  @Override
  public boolean release() {
    return data.release();
  }

  @Override
  public boolean release(final int decrement) {
    return data.release(decrement);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

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

  @Override
  public String toString() {
    return "ZMTPFrame{\"" +
           ZMTPUtils.toString(data) +
           "\"}";
  }
}
