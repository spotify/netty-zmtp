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

package com.spotify.netty4.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;

import static java.lang.Math.min;

/**
 * A writer for encoding ZMTP frames onto a {@link ByteBuf}.
 */
public class ZMTPWriter {

  private final ZMTPWireFormat.Header header;

  private ByteBuf buf;
  private int frameSize;
  private int headerIndex;
  private int contentIndex;

  ZMTPWriter(final ZMTPWireFormat wireFormat) {
    this(wireFormat.header());
  }

  ZMTPWriter(final ZMTPWireFormat.Header header) {
    this.header = header;
  }

  void reset(final ByteBuf buf) {
    this.buf = buf;
  }

  /**
   * Start a new ZMTP frame.
   *
   * @param size Payload size.
   * @param more true if more frames will be written, false if this is the last frame.
   * @return A {@link ByteBuf} for writing the frame payload.
   */
  public ByteBuf frame(final int size, final boolean more) {
    frameSize = size;
    headerIndex = buf.writerIndex();
    header.set(size, size, more);
    header.write(buf);
    contentIndex = buf.writerIndex();
    return buf;
  }

  /**
   * Rewrite the ZMTP frame header, optionally writing a different size or changing the MORE flag.
   * This can be useful when writing a payload where estimating the exact size is expensive but an
   * upper bound can be cheaply computed. E.g. when writing UTF8.
   *
   * @param size New size. This must not be greater than the size provided in the call to {@link
   *             #frame}.
   * @param more true if more frames will be written, false if this is the last frame.
   * @return A {@link ByteBuf} for writing the remainder of the frame payload, if any. The {@link
   * ByteBuf#writerIndex()} will be set to directly after the already written payload, or truncated
   * down to the end of the new smaller payload, if the written payload exceeds the new frame size.
   */
  public ByteBuf reframe(final int size, final boolean more) {
    if (size > frameSize) {
      // Although ByteBufs grow (reallocate) dynamically, the header might end up taking more space,
      // forcing us to move the already written payload. We currently do not implement this.
      throw new IllegalArgumentException("new frame size is greater than original size");
    }
    final int mark = buf.writerIndex();
    final int written = mark - contentIndex;
    if (written < 0) {
      throw new IllegalStateException("written < 0");
    }
    final int newIndex = contentIndex + min(written, size);
    buf.writerIndex(headerIndex);
    header.set(frameSize, size, more);
    header.write(buf);
    buf.writerIndex(newIndex);
    return buf;
  }

  static ZMTPWriter create(final ZMTPVersion version) {
    return new ZMTPWriter(ZMTPWireFormats.wireFormat(version));
  }
}
