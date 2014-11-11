package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

/**
 * A ZMTP20Codec instance is a ChannelUpstreamHandler that, when placed in a ChannelPipeline,
 * will perform a ZMTP/2.0 handshake with the connected peer and replace itself with the proper
 * pipeline components to encode and decode ZMTP frames.
 */
public class ZMTP20Codec extends CodecBase {

  private final boolean interop;
  private boolean splitHandshake;

  /**
   * Construct a ZMTP20Codec with the specified session and optional interoperability behavior.
   * @param session the session that configures this codec
   * @param interop whether this socket should implement the ZMTP/1.0 interoperability handshake
   */
  public ZMTP20Codec(ZMTPSession session, boolean interop) {
    super(session);
    if (session.getSocketType() == null) {
      throw new IllegalArgumentException("ZMTP/2.0 requires a socket type");
    }
    this.interop = interop;
  }

  protected ChannelBuffer onConnect() {
    if (interop) {
      return makeZMTP2CompatSignature();
    } else {
      return makeZMTP2Greeting(true);
    }
  }

  @Override
  boolean inputOutput(final ChannelBuffer buffer, final Channel channel) throws ZMTPException {
    if (splitHandshake) {
      done(2, parseZMTP2Greeting(buffer, false));
      return true;
    }

    if (interop) {
      buffer.markReaderIndex();
      int version = detectProtocolVersion(buffer);
      if (version == 1) {
        buffer.resetReaderIndex();
        // when a ZMTP/1.0 peer is detected, just send the identity bytes. Together
        // with the compatibility signature it makes for a valid ZMTP/1.0 greeting.
        channel.write(ChannelBuffers.wrappedBuffer(session.getLocalIdentity()));
        done(version, ZMTP10Codec.readZMTP1RemoteIdentity(buffer));
        return true;
      } else {
        splitHandshake = true;
        channel.write(makeZMTP2Greeting(false));
        return false;
      }
    } else {
      done(2, parseZMTP2Greeting(buffer, true));
      return true;
    }
  }


  private void done(int version, byte[] remoteIdentity) {
    if (listener != null) {
      listener.handshakeDone(version, remoteIdentity);
    }
  }

  /**
   * Read enough bytes from buffer to deduce the remote protocol version.
   *
   * @param buffer the buffer of data to determine version from
   * @return false if not enough data is available, else true
   * @throws IndexOutOfBoundsException if there is not enough data available in buffer
   */
  static int detectProtocolVersion(final ChannelBuffer buffer) {
    if (buffer.readByte() != (byte)0xff) {
      return 1;
    }
    buffer.skipBytes(8);
    if ((buffer.readByte() & 0x01) == 0) {
      return 1;
    }
    return 2;
  }

  /**
   * Make a ChannelBuffer containing a ZMTP/2.0 greeting, possibly leaving out the 10 initial
   * signature octets if includeSignature is false.
   *
   * @param includeSignature true if a full greeting should be sent, false if the initial 10
   *                         octets should be left out
   * @return a ChannelBuffer containing the greeting
   */
  private ChannelBuffer makeZMTP2Greeting(boolean includeSignature) {
    ChannelBuffer out = ChannelBuffers.dynamicBuffer();
    if (includeSignature) {
      ZMTPUtils.encodeLength(0, out, true);
      // last byte of signature
      out.writeByte(0x7f);
      // protocol revision
    }
    out.writeByte(0x01);
    // socket-type
    ZMTPSocketType socketType = session.getSocketType();
    assert socketType != null;
    out.writeByte(socketType.ordinal());
    // identity
    // the final-short flag octet
    out.writeByte(0x00);
    out.writeByte(session.getLocalIdentity().length);
    out.writeBytes(session.getLocalIdentity());
    return out;
  }

  /**
   * Create and return a ChannelBuffer containing the ZMTP/2.0 compatibility detection signature
   * message as specified in the Backwards Compatibility section of http://rfc.zeromq.org/spec:15
   */
  private ChannelBuffer makeZMTP2CompatSignature() {
    ChannelBuffer out = ChannelBuffers.dynamicBuffer();
    ZMTPUtils.encodeLength(session.getLocalIdentity().length + 1, out, true);
    out.writeByte(0x7f);
    return out;
  }

  static byte[] parseZMTP2Greeting(ChannelBuffer buffer, boolean expectSignature) throws ZMTPException {
    if (expectSignature) {
      if (buffer.readByte() != (byte)0xff) {
        throw new ZMTPException("Illegal ZMTP/2.0 greeting, first octet not 0xff");
      }
      buffer.skipBytes(9);
    }
    // ignoring version number and socket type for now
    buffer.skipBytes(2);
    int val = buffer.readByte();
    if (val != 0x00) {
      String s = String.format("Malformed greeting. Byte 13 expected to be 0x00, was: 0x%02x", val);
      throw new ZMTPException(s);
    }
    int len = buffer.readByte();
    final byte[] identity = new byte[len];
    buffer.readBytes(identity);
    return identity;
  }

}
