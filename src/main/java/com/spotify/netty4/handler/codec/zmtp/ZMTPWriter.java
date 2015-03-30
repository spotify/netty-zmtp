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

/**
 * A writer for encoding ZMTP frames onto a {@link ByteBuf}.
 */
public class ZMTPWriter {

  private final int version;
  private final ByteBuf buf;

  private int headerIndex;
  private int frameSize;

  ZMTPWriter(final int version, final ByteBuf buf) {
    this.version = version;
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
    ZMTPUtils.writeFrameHeader(buf, size, size, more, version);
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
   */
  public void reframe(final int size, final boolean more) {
    if (size > frameSize) {
      throw new IllegalArgumentException("new frame size is greater than original size");
    }
    final int mark = buf.writerIndex();
    buf.writerIndex(headerIndex);
    ZMTPUtils.writeFrameHeader(buf, frameSize, size, more, version);
    buf.writerIndex(mark);
  }
}
