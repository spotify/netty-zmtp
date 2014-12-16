package com.spotify.netty.handler.codec.zmtp;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * A ZMTP10Codec instance is a ChannelUpstreamHandler that, when placed in a ChannelPipeline,
 * will perform a ZMTP/1.0 handshake with the connected peer and replace itself with the proper
 * pipeline components to encode and decode ZMTP frames.
 */
public class ZMTP10Handshaker implements ZMTPHandshaker {

  private final byte[] localIdentity;

  public ZMTP10Handshaker(final byte[] localIdentity) {
    this.localIdentity = localIdentity;
  }

  @Override
  public ByteBuf onConnect() {
    return makeZMTP1Greeting(localIdentity);
  }

  @Override
  public ZMTPHandshake inputOutput(final ByteBuf buffer, final Channel channel) throws ZMTPException {
    final byte[] remoteIdentity = ZMTPUtils.readZMTP1RemoteIdentity(buffer);
    return new ZMTPHandshake(1, remoteIdentity);
  }

  /**
   * Create and return a ByteBuf containing an ZMTP/1.0 greeting based on on the constructor
   * provided session.
   *
   * @return a ByteBuf with a greeting
   */
  private static ByteBuf makeZMTP1Greeting(byte[] localIdentity) {
    ByteBuf out = Unpooled.buffer();
    ZMTPUtils.encodeLength(localIdentity.length + 1, out);
    out.writeByte(0x00);
    out.writeBytes(localIdentity);
    return out;
  }

}
