package com.spotify.netty4.handler.codec.zmtp;


import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.ReplayingDecoder;

/**
 * An abstract base class for common functionality to the ZMTP codecs.
 */
abstract class CodecBase extends ReplayingDecoder<Void> {

  protected final ZMTPSession session;
  protected HandshakeListener listener;

  private final ZMTPMessageEncoder encoder;
  private final ZMTPMessageDecoder decoder;

  CodecBase(final ZMTPSession session,
            final ZMTPMessageEncoder encoder,
            final ZMTPMessageDecoder decoder) {
    this.session = session;
    this.encoder = encoder;
    this.decoder = decoder;
  }

  public CodecBase(final ZMTPSession session) {
    this(session,
         new DefaultZMTPMessageEncoder(session.isEnveloped()),
         new ZMTPIncomingMessageDecoder(session.isEnveloped(), session.sizeLimit())
    );
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {

    setListener(new HandshakeListener() {
      @Override
      public void handshakeDone(int protocolVersion, byte[] remoteIdentity) {
        session.remoteIdentity(remoteIdentity);
        session.actualVersion(protocolVersion);
        updatePipeline(ctx.pipeline(), session);
        ctx.fireChannelActive();
      }
    });

    ctx.writeAndFlush(onConnect());
  }

  abstract ByteBuf onConnect();

  abstract boolean inputOutput(final ByteBuf buffer, final ChannelHandlerContext ctx)
      throws ZMTPException;

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {

    in.markReaderIndex();
    boolean done = inputOutput(in, ctx);
    if (!done) {
      return;
    }

    // This follows the pattern for dynamic pipelines documented in
    // http://netty.io/4.0/api/io/netty/handler/codec/ReplayingDecoder.html
    if (actualReadableBytes() > 0) {
      out.add(in.readBytes(actualReadableBytes()));
    }
  }

  void setListener(HandshakeListener listener) {
    this.listener = listener;
  }


  private void updatePipeline(ChannelPipeline pipeline,
                              ZMTPSession session) {
    final ZMTPMessageParser parser = ZMTPMessageParser.create(
        session.actualVersion(), decoder);
    pipeline.replace(
        this, "zmtp-codec",
        new CombinedChannelDuplexHandler<ZMTPFramingDecoder, ZMTPFramingEncoder>(
            new ZMTPFramingDecoder(parser),
            new ZMTPFramingEncoder(session, encoder)));
  }
}
