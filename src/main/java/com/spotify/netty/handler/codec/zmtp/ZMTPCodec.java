package com.spotify.netty.handler.codec.zmtp;


import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ReplayingDecoder;

import static com.spotify.netty.handler.codec.zmtp.ZMTPUtils.checkNotNull;

/**
 * An abstract base class for common functionality to the ZMTP codecs.
 */
public class ZMTPCodec extends ReplayingDecoder<Void> {

  private final ZMTPSession session;
  private final ZMTPHandshaker handshaker;

  public ZMTPCodec(ZMTPSession session, final ZMTPHandshaker handshaker) {
    this.session = checkNotNull(session, "session");
    this.handshaker = checkNotNull(handshaker, "handshaker");
  }

  public ZMTPCodec(final Builder builder) {
    this.session = null;
    this.handshaker = builder.protocol.handshaker(new ZMTPSession(builder.connectionType));
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    this.session.channel(ctx.channel());
    ctx.writeAndFlush(handshaker.onConnect());
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {

    in.markReaderIndex();
    final ZMTPHandshake handshake = handshaker.inputOutput(in, ctx.channel());
    if (handshake == null) {
      return;
    }

    session.remoteIdentity(handshake.remoteIdentity());
    session.actualVersion(handshake.protocolVersion());
    updatePipeline(ctx.pipeline(), session);
    ctx.fireChannelActive();

    // This follows the pattern for dynamic pipelines documented in
    // http://netty.io/4.0/api/io/netty/handler/codec/ReplayingDecoder.html
    if (actualReadableBytes() > 0) {
      out.add(in.readBytes(actualReadableBytes()));
    }
  }

  private void updatePipeline(ChannelPipeline pipeline,
                              ZMTPSession session) {
    pipeline.addAfter(pipeline.context(this).name(), "zmtpEncoder",
                      new ZMTPFramingEncoder(session));
    pipeline.addAfter("zmtpEncoder", "zmtpDecoder",
                      new ZMTPFramingDecoder(session));
    pipeline.remove(this);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private ZMTPProtocol protocol = ZMTPProtocol.ZMTP20;
    private ZMTPConnectionType connectionType = ZMTPConnectionType.ADDRESSED;
    private ZMTPSocketType socketType;
    private byte[] localIdentity;
    private boolean interop;

    private Builder() {
    }

    public Builder protocol(final ZMTPProtocol procotol) {
      this.protocol = procotol;
      return this;
    }

    public Builder connectionType(final ZMTPConnectionType connectionType) {
      this.connectionType = connectionType;
      return this;
    }

    public Builder socketType(final ZMTPSocketType socketType) {
      this.socketType = socketType;
      return this;
    }

    public ZMTPCodec build() {
      return new ZMTPCodec(this);
    }

    public Builder interop(final boolean interop) {
      this.interop = interop;
      return this;
    }
  }
}
