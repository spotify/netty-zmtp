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
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPFrame.EMPTY_FRAME;

public class ZMTPIncomingMessageDecoder implements ZMTPMessageDecoder {

  private final boolean enveloped;
  private final long limit;

  private boolean delimited;
  private boolean truncated;
  private long messageSize;
  private int frameLength;

  private List<ZMTPFrame> head;
  private List<ZMTPFrame> tail;
  private List<ZMTPFrame> part;

  public ZMTPIncomingMessageDecoder(final boolean enveloped, final long limit) {
    this.enveloped = enveloped;
    this.limit = limit;
    reset();
  }

  /**
   * Reset parser in preparation for the next message.
   */
  private void reset() {
    if (enveloped) {
      head = new ArrayList<ZMTPFrame>(3);
      tail = new ArrayList<ZMTPFrame>(3);
      part = head;
    } else {
      head = Collections.emptyList();
      tail = new ArrayList<ZMTPFrame>(3);
      part = tail;
    }
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
    if (frameLength > 0) {
      final ByteBuf frame = data.readSlice(frameLength);
      frame.retain();
      part.add(new ZMTPFrame(frame));
    } else if (part == tail) {
      part.add(EMPTY_FRAME);
    } else {
      delimited = true;
      part = tail;
    }
  }

  @Override
  public void finish(final List<Object> out) {
    final List<ZMTPFrame> envelope;
    final List<ZMTPFrame> content;

    // If we're expecting enveloped messages but didn't get a delimiter, then we treat that as a
    // message without an envelope and assign the received frames to the content part of the
    // message instead of the envelope. This is to allow the parser to deal with situations where
    // we're not really sure if we're going to get enveloped messages or not.
    if (enveloped && !delimited && !truncated) {
      envelope = Collections.emptyList();
      content = head;
    } else {
      envelope = head;
      content = tail;
    }

    final ZMTPMessage message = new ZMTPMessage(envelope, content);
    final ZMTPIncomingMessage incomingMessage = new ZMTPIncomingMessage(
        message, truncated, messageSize);

    reset();

    out.add(incomingMessage);
  }
}
