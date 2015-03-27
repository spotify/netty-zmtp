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

public class ZMTPMessageDecoder implements ZMTPDecoder {

  private static final long DEFAULT_SIZE_LIMIT = 4 * 1024 * 1024;

  private final long limit;

  private boolean delimited;
  private boolean truncated;
  private long messageSize;
  private int frameLength;

  private List<ZMTPFrame> frames;

  public ZMTPMessageDecoder() {
    this(DEFAULT_SIZE_LIMIT);
  }

  public ZMTPMessageDecoder(final long limit) {
    this.limit = limit;
    reset();
  }

  /**
   * Reset parser in preparation for the next message.
   */
  private void reset() {
    frames = new ArrayList<ZMTPFrame>();
    delimited = false;
    truncated = false;
    messageSize = 0;
    frameLength = 0;
  }

  @Override
  public void header(final int length, final boolean more, final List<Object> out) {
    frameLength = length;
    messageSize += length;
    if (messageSize > limit) {
      truncated = true;
    }
  }

  @Override
  public void content(final ByteBuf data, final List<Object> out) {
    if (data.readableBytes() < frameLength) {
      return;
    }
    final ByteBuf frame = data.readSlice(frameLength);
    frame.retain();
    frames.add(new ZMTPFrame(frame));
  }

  @Override
  public void finish(final List<Object> out) {
    final ZMTPMessage message = new ZMTPMessage(frames);
    final ZMTPIncomingMessage incomingMessage = new ZMTPIncomingMessage(
        message, truncated, messageSize);
    reset();
    out.add(incomingMessage);
  }
}
