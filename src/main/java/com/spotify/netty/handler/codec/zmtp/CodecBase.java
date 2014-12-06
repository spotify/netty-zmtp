package com.spotify.netty.handler.codec.zmtp;


import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ReplayingDecoder;

/**
 * An abstract base class for common functionality to the ZMTP codecs.
 */
abstract class CodecBase extends ReplayingDecoder<Void> {

  protected final ZMTPSession session;
  protected HandshakeListener listener;

  CodecBase(ZMTPSession session) {
    this.session = session;
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

    final Channel channel = ctx.channel();

    channel.writeAndFlush(onConnect());
    this.session.channel(channel);
  }

  abstract ByteBuf onConnect();

  abstract boolean inputOutput(final ByteBuf buffer, final Channel channel) throws ZMTPException;

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {

    in.markReaderIndex();
    boolean done = inputOutput(in, ctx.channel());
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
    pipeline.addAfter(pipeline.context(this).name(), "zmtpEncoder",
                      new ZMTPFramingEncoder(session));
    pipeline.addAfter("zmtpEncoder", "zmtpDecoder",
                      new ZMTPFramingDecoder(session));
    pipeline.remove(this);
  }

  /**
   * Parse and return the remote identity octets from a ZMTP/1.0 greeting.
   */
  static byte[] readZMTP1RemoteIdentity(final ByteBuf buffer) throws ZMTPException {
    buffer.markReaderIndex();

    final long len = ZMTPUtils.decodeLength(buffer);
    if (len > 256) {
      // spec says the ident string can be up to 255 chars
      throw new ZMTPException("Remote identity longer than the allowed 255 octets");
    }

    // Bail out if there's not enough data
    if (len == -1 || buffer.readableBytes() < len) {
      buffer.resetReaderIndex();
      throw new IndexOutOfBoundsException("not enough data");
    }
    // skip the flags byte
    buffer.skipBytes(1);

    if (len == 1) {
      return null;
    }
    final byte[] identity = new byte[(int)len - 1];
    buffer.readBytes(identity);
    return identity;
  }
}
