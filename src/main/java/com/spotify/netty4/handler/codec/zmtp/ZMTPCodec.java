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


import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.ReplayingDecoder;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;
import static io.netty.util.CharsetUtil.UTF_8;

/**
 * A ZMTP codec for Netty.
 */
public class ZMTPCodec extends ReplayingDecoder<Void> {

  private final ZMTPSession session;
  private final ZMTPHandshaker handshaker;

  private final ZMTPEncoder encoder;
  private final ZMTPDecoder decoder;

  private ZMTPCodec(final Builder builder) {
    final ZMTPProtocol protocol = checkNotNull(builder.protocol, "protocol");
    this.session = new ZMTPSession(protocol, builder.localIdentity);
    this.handshaker = protocol.handshaker(session);
    this.encoder = (builder.encoder == null)
                   ? new ZMTPMessageEncoder()
                   : builder.encoder;
    this.decoder = (builder.decoder == null)
                   ? new ZMTPMessageDecoder()
                   : builder.decoder;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    ctx.writeAndFlush(handshaker.greeting());
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {

    final ZMTPHandshake handshake = handshaker.handshake(in, ctx);
    if (handshake == null) {
      return;
    }

    session.handshakeDone(handshake);

    updatePipeline(ctx.pipeline(), session);

    // This follows the pattern for dynamic pipelines documented in
    // http://netty.io/4.0/api/io/netty/handler/codec/ReplayingDecoder.html
    if (actualReadableBytes() > 0) {
      out.add(in.readBytes(actualReadableBytes()));
    }

    ctx.fireUserEventTriggered(session);
  }

  private void updatePipeline(ChannelPipeline pipeline,
                              ZMTPSession session) {
    final ZMTPMessageParser parser = ZMTPMessageParser.create(session.actualVersion(), decoder);
    final ChannelHandler handler =
        new CombinedChannelDuplexHandler<ZMTPFramingDecoder, ZMTPFramingEncoder>(
            new ZMTPFramingDecoder(parser),
            new ZMTPFramingEncoder(session, encoder));
    pipeline.replace(this, "zmtp-codec", handler);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ZMTPCodec of(final ZMTPProtocol protocol) {
    return builder().protocol(protocol).build();
  }

  public static class Builder {

    private ZMTPProtocol protocol;
    private ByteBuffer localIdentity = ByteBuffer.allocate(0);
    private ZMTPEncoder encoder;
    private ZMTPDecoder decoder;

    private Builder() {
    }

    public Builder protocol(final ZMTPProtocol protocol) {
      this.protocol = protocol;
      return this;
    }

    public Builder localIdentity(final CharSequence localIdentity) {
      return localIdentity(UTF_8.encode(localIdentity.toString()));
    }

    public Builder localIdentity(final byte[] localIdentity) {
      return localIdentity(ByteBuffer.wrap(localIdentity));
    }

    public Builder localIdentity(final ByteBuffer localIdentity) {
      this.localIdentity = localIdentity;
      return this;
    }

    public Builder encoder(final ZMTPEncoder encoder) {
      this.encoder = encoder;
      return this;
    }

    public Builder decoder(final ZMTPDecoder decoder) {
      this.decoder = decoder;
      return this;
    }

    public ZMTPCodec build() {
      return new ZMTPCodec(this);
    }
  }
}
