/*
 * Copyright (c) 2012-2015 Spotify AB
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

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class ZMTPMessageDecoder implements ZMTPDecoder {

  public static final Factory FACTORY = new Factory() {
    @Override
    public ZMTPDecoder decoder(final ZMTPSession session) {
      return new ZMTPMessageDecoder();
    }
  };

  private static final ByteBuf DELIMITER = Unpooled.EMPTY_BUFFER;

  private final List<ByteBuf> frames = new ArrayList<ByteBuf>();
  private int frameLength;

  /**
   * Reset parser in preparation for the next message.
   */
  private void reset() {
    frames.clear();
    frameLength = 0;
  }

  @Override
  public void header(final ChannelHandlerContext ctx, final long length, final boolean more,
                     final List<Object> out) {
    frameLength = (int) length;
  }

  @Override
  public void content(final ChannelHandlerContext ctx, final ByteBuf data, final List<Object> out) {
    // Wait for more data?
    if (data.readableBytes() < frameLength) {
      return;
    }

    if (frameLength == 0) {
      frames.add(DELIMITER);
      return;
    }

    final ByteBuf frame = data.readSlice(frameLength);
    frame.retain();
    frames.add(frame);
  }

  @Override
  public void finish(final ChannelHandlerContext ctx, final List<Object> out) {
    final ZMTPMessage message = ZMTPMessage.from(frames);
    reset();
    out.add(message);
  }

  @Override
  public void close() {
    for (final ByteBuf frame : frames) {
      frame.release();
    }
    frames.clear();
  }
}
