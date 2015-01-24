package com.spotify.netty4.handler.codec.zmtp;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * A ZMTP10Codec instance is a ChannelUpstreamHandler that, when placed in a ChannelPipeline,
 * will perform a ZMTP/1.0 handshake with the connected peer and replace itself with the proper
 * pipeline components to encode and decode ZMTP frames.
 */
public class ZMTP10Codec extends CodecBase {

  /**
   * Constructs a codec with the specified session.
   *
   * @param session the session that configures this codec.
   */
  public ZMTP10Codec(ZMTPSession session) {
    super(session);
  }

  public ZMTP10Codec(final ZMTPSession session,
                     final ZMTPMessageEncoder encoder,
                     final ZMTPMessageDecoder decoder) {
    super(session, encoder, decoder);
  }

  @Override
  protected ByteBuf onConnect() {
    return makeZMTP1Greeting(session.localIdentity());
  }

  @Override
  boolean inputOutput(final ByteBuf buffer, final ChannelHandlerContext ctx) throws ZMTPException {
    byte[] remoteIdentity = readZMTP1RemoteIdentity(buffer);
    if (listener != null) {
      listener.handshakeDone(1, remoteIdentity);
    }
    return true;
  }

  /**
   * Create and return a ByteBuf containing an ZMTP/1.0 greeting based on on the constructor
   * provided session.
   *
   * @return a ByteBuf with a greeting
   */
  private static ByteBuf makeZMTP1Greeting(byte[] localIdentity) {
    ByteBuf out = Unpooled.buffer();
    ZMTPUtils.encodeZMTP1Length(localIdentity.length + 1, out);
    out.writeByte(0x00);
    out.writeBytes(localIdentity);
    return out;
  }

}
