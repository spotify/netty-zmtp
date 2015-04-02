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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;
import static java.lang.String.format;

class ZMTP20Protocol implements ZMTPProtocol {

  @Override
  public ZMTPHandshaker handshaker(final ZMTPConfig config) {
    return new Handshaker(config.socketType(), config.interop(), config.localIdentity());
  }

  @Override
  public ZMTPVersion version() {
    return ZMTPVersion.ZMTP20;
  }

  static class Handshaker implements ZMTPHandshaker {

    private final ZMTPSocketType socketType;
    private final boolean interop;
    private final ByteBuffer localIdentity;

    private boolean splitHandshake;

    /**
     * Construct a ZMTP20Handshaker with the specified session and optional interoperability
     * behavior.
     *
     * @param socketType    The ZMTP/2.0 socket type.
     * @param interop       whether this socket should implement the ZMTP/1.0 interoperability
     *                      handshake
     * @param localIdentity Local identity. An identity will be generated if null.
     */
    Handshaker(final ZMTPSocketType socketType, boolean interop, final ByteBuffer localIdentity) {
      this.socketType = checkNotNull(socketType, "ZMTP/2.0 requires a socket type");
      this.localIdentity = checkNotNull(localIdentity, "localIdentity");
      this.interop = interop;
    }

    @Override
    public ByteBuf greeting() {
      if (interop) {
        return makeZMTP2CompatSignature();
      } else {
        return makeZMTP2Greeting(true);
      }
    }

    @Override
    public ZMTPHandshake handshake(final ByteBuf in, final ChannelHandlerContext ctx)
        throws ZMTPException {
      if (splitHandshake) {
        return ZMTPHandshake.of(ZMTPVersion.ZMTP20, ByteBuffer.wrap(parseZMTP2Greeting(in, false)));
      }

      if (interop) {
        in.markReaderIndex();
        final ZMTPVersion version = detectProtocolVersion(in);
        switch (version) {
          case ZMTP10:
            in.resetReaderIndex();
            // when a ZMTP/1.0 peer is detected, just send the identity bytes. Together
            // with the compatibility signature it makes for a valid ZMTP/1.0 greeting.
            ctx.writeAndFlush(Unpooled.wrappedBuffer(localIdentity));
            final byte[] remoteIdentity = ZMTP10WireFormat.readIdentity(in);
            assert remoteIdentity != null;
            return ZMTPHandshake.of(version, ByteBuffer.wrap(remoteIdentity));
          case ZMTP20:
            splitHandshake = true;
            ctx.writeAndFlush(makeZMTP2Greeting(false));
            return null;
          default:
            throw new ZMTPException("Unknown ZMTP version: " + version);
        }
      } else {
        return ZMTPHandshake.of(ZMTPVersion.ZMTP20, ByteBuffer.wrap(parseZMTP2Greeting(in, true)));
      }
    }

    /**
     * Read enough bytes from buffer to deduce the remote protocol version.
     *
     * @param buffer the buffer of data to determine version from.
     * @return The detected {@link ZMTPVersion}.
     * @throws IndexOutOfBoundsException if there is not enough data available in buffer.
     */
    static ZMTPVersion detectProtocolVersion(final ByteBuf buffer) {
      if (buffer.readByte() != (byte) 0xff) {
        return ZMTPVersion.ZMTP10;
      }
      buffer.skipBytes(8);
      if ((buffer.readByte() & 0x01) == 0) {
        return ZMTPVersion.ZMTP10;
      }
      // TODO (dano): parse revision & socket type here
      return ZMTPVersion.ZMTP20;
    }

    /**
     * Make a ByteBuf containing a ZMTP/2.0 greeting, possibly leaving out the 10 initial signature
     * octets if includeSignature is false.
     *
     * @param includeSignature true if a full greeting should be sent, false if the initial 10
     *                         octets should be left out
     * @return a ByteBuf containing the greeting
     */
    private ByteBuf makeZMTP2Greeting(boolean includeSignature) {
      ByteBuf out = Unpooled.buffer();
      if (includeSignature) {
        ZMTP10WireFormat.writeLength(0, out, true);
        // last byte of signature
        out.writeByte(0x7f);
        // protocol revision
      }
      out.writeByte(0x01);
      // socket-type
      out.writeByte(socketType.ordinal());
      // identity
      // the final-short flag octet
      out.writeByte(0x00);
      out.writeByte(localIdentity.remaining());
      out.writeBytes(localIdentity.duplicate());
      return out;
    }

    /**
     * Create and return a ByteBuf containing the ZMTP/2.0 compatibility detection signature message
     * as specified in the Backwards Compatibility section of http://rfc.zeromq.org/spec:15
     */
    private ByteBuf makeZMTP2CompatSignature() {
      ByteBuf out = Unpooled.buffer();
      ZMTP10WireFormat.writeLength(localIdentity.remaining() + 1, out, true);
      out.writeByte(0x7f);
      return out;
    }

    static byte[] parseZMTP2Greeting(ByteBuf buffer, boolean expectSignature) throws ZMTPException {
      if (expectSignature) {
        if (buffer.readByte() != (byte) 0xff) {
          throw new ZMTPException("Illegal ZMTP/2.0 greeting, first octet not 0xff");
        }
        buffer.skipBytes(9);
      }
      final int revision = buffer.readByte();
      if (revision != ZMTPVersion.ZMTP20.revision()) {
        throw new ZMTPException("Unsupported ZMTP revision: " + revision);
      }
      // Skip socket type for now
      buffer.skipBytes(1);
      final int val = buffer.readByte();
      if (val != 0x00) {
        throw new ZMTPException(format(
            "Malformed greeting. Byte 13 expected to be 0x00, was: 0x%02x", val));
      }
      int len = buffer.readByte();
      final byte[] identity = new byte[len];
      buffer.readBytes(identity);
      return identity;
    }

  }
}
