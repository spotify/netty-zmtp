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
import java.nio.channels.ClosedChannelException;
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

  public ZMTPCodec(final ZMTPSession session) {
    this.config = session.config();
    this.session = checkNotNull(session, "session");
    this.handshaker = config.protocol().handshaker(config);
  }

  /**
   * Get the {@link ZMTPSession} for this codec.
   */
  public ZMTPSession session() {
    return session;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    ctx.writeAndFlush(handshaker.greeting());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if (!session.handshakeFuture().isDone()) {
      session.handshakeFailure(new ClosedChannelException());
      ctx.fireUserEventTriggered(new ZMTPHandshakeFailure(session));
    }
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {

    // Discard input if handshake failed. It is expected that the user will close the channel.
    if (session.handshakeFuture().isDone()) {
      assert !session.handshakeFuture().isSuccess();
      in.skipBytes(in.readableBytes());
    }

    // Shake hands
    final ZMTPHandshake handshake;
    try {
      handshake = handshaker.handshake(in, ctx);
      if (handshake == null) {
        // Handshake is not yet done. Await more input.
        return;
      }
    } catch (Exception e) {
      session.handshakeFailure(e);
      ctx.fireUserEventTriggered(new ZMTPHandshakeFailure(session));
      throw e;
    }

    // Handshake is done.
    session.handshakeSuccess(handshake);

    // Replace this handler with the framing encoder and decoder
    if (actualReadableBytes() > 0) {
      out.add(in.readBytes(actualReadableBytes()));
    }
    final ZMTPDecoder decoder = config.decoder().decoder(session);
    final ZMTPEncoder encoder = config.encoder().encoder(session);
    final ZMTPWireFormat wireFormat = ZMTPWireFormats.wireFormat(session.negotiatedVersion());
    final ChannelHandler handler =
        new CombinedChannelDuplexHandler<ZMTPFramingDecoder, ZMTPFramingEncoder>(
            new ZMTPFramingDecoder(wireFormat, decoder),
            new ZMTPFramingEncoder(wireFormat, encoder));
    ctx.pipeline().replace(this, ctx.name(), handler);

    // Tell the user that the handshake is complete
    ctx.fireUserEventTriggered(new ZMTPHandshakeSuccess(session, handshake));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ZMTPCodec from(final ZMTPConfig config) {
    return new ZMTPCodec(ZMTPSession.from(config));
  }

  public static ZMTPCodec of(final ZMTPSocketType socketType) {
    return builder().socketType(socketType).build();
  }

  public static ZMTPCodec from(final ZMTPSession session) {
    return new ZMTPCodec(session);
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

    public Builder identityGenerator(final ZMTPIdentityGenerator identityGenerator) {
      config.identityGenerator(identityGenerator);
      return this;
    }

    public ZMTPCodec build() {
      return ZMTPCodec.from(config.build());
    }
  }
}
