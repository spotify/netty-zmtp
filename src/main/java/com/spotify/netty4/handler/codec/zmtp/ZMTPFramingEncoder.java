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


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * Netty encoder for ZMTP messages.
 */
class ZMTPFramingEncoder extends ChannelOutboundHandlerAdapter {

  private final ZMTPMessageEncoder encoder;
  private final ZMTPWriter writer;

  public ZMTPFramingEncoder(final ZMTPSession session) {
    this(session, new DefaultZMTPMessageEncoder(session.isEnveloped()));
  }

  public ZMTPFramingEncoder(final ZMTPSession session, final ZMTPMessageEncoder encoder) {
    this(session, encoder, new ZMTPWriter(session.actualVersion()));
  }

  public ZMTPFramingEncoder(final ZMTPSession session, final ZMTPMessageEncoder encoder,
                            final ZMTPWriter writer) {
    if (session == null) {
      throw new NullPointerException("session");
    }
    if (encoder == null) {
      throw new NullPointerException("encoder");
    }
    if (writer == null) {
      throw new NullPointerException("writer");
    }
    this.encoder = encoder;
    this.writer = writer;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    writer.alloc(ctx.alloc());
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
      throws Exception {
    writer.reset();
    encoder.encode(msg, writer);
    final ByteBuf output = writer.finish();
    ReferenceCountUtil.release(msg);
    ctx.write(output, promise);
  }
}
