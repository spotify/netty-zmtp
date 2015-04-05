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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.ReplayingDecoder;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;

/**
 * A ZMTP codec for Netty.
 *
 * Note: A single codec instance is not {@link Sharable} among multiple {@link Channel} instances.
 */
public class ZMTPCodec extends ReplayingDecoder<Void> {

  private final ZMTPSession session;
  private final ZMTPHandshaker handshaker;

  private final ZMTPConfig config;

  private ZMTPCodec(final ZMTPConfig config) {
    this.config = checkNotNull(config, "config");
    this.session = new ZMTPSession(config);
    this.handshaker = config.protocol().handshaker(config);
  }

  public ZMTPSession session() {
    return session;
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

    final ZMTPDecoder decoder = config.decoder().decoder(config);
    final ZMTPEncoder encoder = config.encoder().encoder(config);
    final ZMTPWireFormat wireFormat = ZMTPWireFormats.wireFormat(session.negotiatedVersion());
    final ChannelHandler handler =
        new CombinedChannelDuplexHandler<ZMTPFramingDecoder, ZMTPFramingEncoder>(
            new ZMTPFramingDecoder(wireFormat, decoder),
            new ZMTPFramingEncoder(wireFormat, encoder));
    ctx.pipeline().replace(this, ctx.name(), handler);

    // This follows the pattern for dynamic pipelines documented in
    // http://netty.io/4.0/api/io/netty/handler/codec/ReplayingDecoder.html
    if (actualReadableBytes() > 0) {
      out.add(in.readBytes(actualReadableBytes()));
    }

    ctx.fireUserEventTriggered(session);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final ZMTPConfig.Builder config = ZMTPConfig.builder();

    private Builder() {
    }

    public Builder protocol(final ZMTPProtocol protocol) {
      config.protocol(protocol);
      return this;
    }

    public Builder interop(final boolean interop) {
      config.interop(interop);
      return this;
    }

    public Builder socketType(final ZMTPSocketType socketType) {
      config.socketType(socketType);
      return this;
    }

    public Builder localIdentity(final CharSequence localIdentity) {
      config.localIdentity(localIdentity);
      return this;
    }

    public Builder localIdentity(final byte[] localIdentity) {
      config.localIdentity(localIdentity);
      return this;
    }

    public Builder localIdentity(final ByteBuffer localIdentity) {
      config.localIdentity(localIdentity);
      return this;
    }

    public Builder encoder(final ZMTPEncoder.Factory encoder) {
      config.encoder(encoder);
      return this;
    }

    public Builder encoder(final Class<? extends ZMTPEncoder> encoder) {
      config.encoder(encoder);
      return this;
    }

    public Builder decoder(final ZMTPDecoder.Factory decoder) {
      config.decoder(decoder);
      return this;
    }

    public Builder decoder(final Class<? extends ZMTPDecoder> decoder) {
      config.decoder(decoder);
      return this;
    }

    public ZMTPCodec build() {
      return new ZMTPCodec(config.build());
    }
  }
}
