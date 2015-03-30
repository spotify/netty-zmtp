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

import static java.lang.Math.min;

public class ZMTPMessageDecoder implements ZMTPDecoder {

  private final long MAX_LIMIT = Integer.MAX_VALUE;

  public static final Factory FACTORY = new Factory() {
    @Override
    public ZMTPDecoder decoder(final ZMTPConfig config) {
      return new ZMTPMessageDecoder();
    }
  };

  public static final long DEFAULT_SIZE_LIMIT = 4 * 1024 * 1024;

  private final int limit;

  private final List<ByteBuf> frames = new ArrayList<ByteBuf>();
  private boolean truncated;
  private long messageSize;
  private long frameRemaining;
  private int readLength;
  private boolean frameRead;

  public ZMTPMessageDecoder() {
    this(DEFAULT_SIZE_LIMIT);
  }

  public ZMTPMessageDecoder(final long limit) {
    if (limit < 0 || limit > MAX_LIMIT) {
      throw new IllegalArgumentException("limit");
    }
    this.limit = (int) limit;
    reset();
  }

  /**
   * Reset parser in preparation for the next message.
   */
  private void reset() {
    frames.clear();
    truncated = false;
    messageSize = 0;
    readLength = 0;
    frameRemaining = 0;
    frameRead = false;
  }

  @Override
  public void header(final long length, final boolean more, final List<Object> out) {
    assert frameRemaining == 0;

    final long newMessageSize = messageSize + length;

    // Truncate?
    if (newMessageSize > limit) {
      truncated = true;
      assert messageSize < limit;
      readLength = (int) min(length, limit - messageSize);
    } else {
      readLength = (int) length;
    }

    frameRead = false;
    frameRemaining = length;
    messageSize = newMessageSize;
  }

  @Override
  public void content(final ByteBuf data, final List<Object> out) {
    // Truncating?
    if (frameRead) {
      assert truncated;
      assert frameRemaining > 0;
      final int n = (int) min(data.readableBytes(), frameRemaining);
      data.skipBytes(n);
      frameRemaining -= n;
      return;
    }

    // Wait for more data?
    if (data.readableBytes() < readLength) {
      return;
    }

    // Read the (potentially truncated) frame in one slice
    final ByteBuf frame = data.readSlice(readLength);
    frameRemaining -= readLength;
    frameRead = true;
    frame.retain();
    frames.add(frame);
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
