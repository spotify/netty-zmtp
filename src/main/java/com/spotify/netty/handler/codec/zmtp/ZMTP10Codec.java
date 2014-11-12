package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

/**
 * A ZMTP10Codec instance is a ChannelUpstreamHandler that, when placed in a ChannelPipeline,
 * will perform a ZMTP/1.0 handshake with the connected peer and replace itself with the proper
 * pipeline components to encode and decode ZMTP frames.
 */
public class ZMTP10Codec extends CodecBase {

  /**
   * Constructs a codec with the specified local identity. If identity is null, the connection type
   * of connections using this codec is treated as ZMTPConnectionType.Broadcast and neither
   * sent nor received frames will be enveloped.
   *
   * @param session the session that configures this codec
   */
  public ZMTP10Codec(ZMTPSession session) {
    super(session);
  }

  @Override
  protected ChannelBuffer onConnect() {
    return makeZMTP1Greeting(session.localIdentity());
  }

  @Override
  boolean inputOutput(final ChannelBuffer buffer, final Channel channel) throws ZMTPException {
    byte[] remoteIdentity = readZMTP1RemoteIdentity(buffer);
    if (listener != null) {
      listener.handshakeDone(1, remoteIdentity);
    }
    return true;
  }

  /**
   * Create and return a ChannelBuffer containing an ZMTP/1.0 greeting based on on the constructor
   * provided session.
   *
   * @return a ChannelBuffer with a greeting
   */
  private static ChannelBuffer makeZMTP1Greeting(byte[] localIdentity) {
    ChannelBuffer out = ChannelBuffers.dynamicBuffer();
    ZMTPUtils.encodeLength(localIdentity.length + 1, out);
    out.writeByte(0x00);
    out.writeBytes(localIdentity);
    return out;
  }

}
