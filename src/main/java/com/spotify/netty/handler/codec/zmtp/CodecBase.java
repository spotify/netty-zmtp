package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;

/**
 * An abstract base class for common functionality to the ZMTP codecs.
 */
abstract class CodecBase extends ReplayingDecoder<VoidEnum>  {

  private final ZMTPSession session;
  private final boolean enveloped;
  protected final byte[] localIdentity;
  protected HandshakeListener listener;

  CodecBase(byte[] localIdentity) {
    this.localIdentity = localIdentity;
    boolean enveloped = localIdentity != null;
    this.session = new ZMTPSession(
        enveloped ? ZMTPConnectionType.Addressed : ZMTPConnectionType.Broadcast);
    this.enveloped = enveloped;
  }

  @Override
  public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e)
      throws Exception {

    setListener(new HandshakeListener() {
      @Override
      public void handshakeDone(int protocolVersion, byte[] remoteIdentity) {
        session.setRemoteIdentity(remoteIdentity);
        session.setProtocolVersion(protocolVersion);
        updatePipeline(ctx.getPipeline(), protocolVersion, session, enveloped);
        ctx.sendUpstream(e);
      }
    });

    Channel channel = e.getChannel();

    channel.write(onConnect());
    this.session.setChannel(e.getChannel());
  }

  abstract ChannelBuffer onConnect();

  abstract ChannelBuffer inputOutput(final ChannelBuffer buffer) throws ZMTPException;

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer,
                          VoidEnum _) throws ZMTPException {
    buffer.markReaderIndex();
    ChannelBuffer toSend = inputOutput(buffer);
    while (toSend != null) {
      ctx.getChannel().write(toSend);
      toSend = inputOutput(buffer);
    }
    // This follows the pattern for dynamic pipelines documented in
    // http://netty.io/3.6/api/org/jboss/netty/handler/codec/replay/ReplayingDecoder.html
    if (actualReadableBytes() > 0) {
      return buffer.readBytes(actualReadableBytes());
    }
    return null;
  }

  void setListener(HandshakeListener listener) {
    this.listener = listener;
  }


  private void updatePipeline(ChannelPipeline pipeline, int version,
                              ZMTPSession session, boolean enveloped) {
    pipeline.addAfter(pipeline.getContext(this).getName(), "zmtpEncoder",
                      new ZMTPFramingEncoder(version, enveloped));
    pipeline.addAfter("zmtpEncoder", "zmtpDecoder",
                      new ZMTPFramingDecoder(version, enveloped, session));
    pipeline.remove(this);
  }

  /**
   * Parse and return the remote identity octets from a ZMTP/1.0 greeting.
   */
  static byte[] readZMTP1RemoteIdentity(final ChannelBuffer buffer) throws ZMTPException {
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
