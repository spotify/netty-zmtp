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


import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * Netty encoder for ZMTP messages.
 */
class ZMTPFramingEncoder extends MessageToMessageEncoder<ZMTPMessage> {

  private final ZMTPSession session;
  private final ByteBufAllocator allocator;

  public ZMTPFramingEncoder(final ZMTPSession session) {
    this(session, PooledByteBufAllocator.DEFAULT);
  }

  public ZMTPFramingEncoder(final ZMTPSession session, final ByteBufAllocator allocator) {
    this.session = session;
    this.allocator = allocator;
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final ZMTPMessage message,
                        final List<Object> out)
      throws Exception {
    final int size = ZMTPUtils.messageSize(message, session.isEnveloped(), session.actualVersion());
    final ByteBuf buffer = allocator.directBuffer(size);
    ZMTPUtils.writeMessage(message, buffer, session.isEnveloped(), session.actualVersion());
    out.add(buffer);
  }
}
