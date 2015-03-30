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

public class ZMTPMessageEncoder implements ZMTPEncoder {

  public static final ZMTPEncoderFactory FACTORY = new ZMTPEncoderFactory() {
    @Override
    public ZMTPEncoder encoder(final ZMTPConfig config) {
      return new ZMTPMessageEncoder();
    }
  };

  @Override
  public void estimate(final Object msg, final ZMTPEstimator estimator) {
    final ZMTPMessage message = (ZMTPMessage) msg;
    for (final ByteBuf frame : message.frames()) {
      estimator.frame(frame.readableBytes());
    }
  }

  @Override
  public void encode(final Object msg, final ZMTPWriter writer) {
    final ZMTPMessage message = (ZMTPMessage) msg;

    for (int i = 0; i < message.frames().size(); i++) {
      final ByteBuf frame = message.frame(i);
      final boolean more = i < message.frames().size() - 1;
      final ByteBuf dst = writer.frame(frame.readableBytes(), more);
      dst.writeBytes(frame, frame.readerIndex(), frame.readableBytes());
    }
  }
}
