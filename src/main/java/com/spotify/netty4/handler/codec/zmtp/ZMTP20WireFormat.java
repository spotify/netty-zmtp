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

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;
import static java.lang.String.format;

/**
 * Tools for reading and writing the ZMTP/2.0 wire format, greeting and message frames.
 */
class ZMTP20WireFormat implements ZMTPWireFormat {

  static final byte FINAL_FLAG = 0x0;
  static final byte LONG_FLAG = 0x02;
  static final byte MORE_FLAG = 0x1;

  @Override
  public int frameLength(final int content) {
    if (content < 256) {
      return 1 + 1 + content;
    } else {
      return 1 + 8 + content;
    }
  }

  @Override
  public Header header() {
    return new ZMTP20Header();
  }

  /**
   * Read a ZMTP/2.0 greeting.
   *
   * @param in The buffer to read the greeting from.
   * @return A {@link com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.Greeting}.
   * @throws ZMTPParsingException      If the greeting is malformed.
   * @throws IndexOutOfBoundsException If there is not enough readable bytes to read an entire
   *                                   greeting.
   */
  static Greeting readGreeting(ByteBuf in)
      throws ZMTPParsingException {
    if (in.readByte() != (byte) 0xff) {
      throw new ZMTPParsingException("Illegal ZMTP/2.0 greeting, first octet not 0xff");
    }
    in.skipBytes(9);
    return readGreetingBody(in);
  }

  /**
   * Read a ZMTP/2.0 greeting body.
   *
   * @param in The buffer to read the greeting from.
   * @return A {@link com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.Greeting}.
   * @throws ZMTPParsingException      If the greeting is malformed.
   * @throws IndexOutOfBoundsException If there is not enough readable bytes to read an entire
   *                                   greeting.
   */
  static Greeting readGreetingBody(final ByteBuf in) throws ZMTPParsingException {
    final int revision = in.readByte();
    final ZMTPSocketType socketType = readSocketType(in);
    final int flags = in.readByte();
    if (flags != 0x00) {
      throw new ZMTPParsingException(format(
          "Malformed ZMTP/2.0 greeting. Flags (byte 13) expected to be 0x00, was 0x%02x", flags));
    }
    final int len = in.readByte();
    final byte[] identity = new byte[len];
    in.readBytes(identity);
    return new Greeting(revision, socketType, ByteBuffer.wrap(identity));
  }

  /**
   * Read a ZMTP/2.0 socket type.
   *
   * @param in The buffer to read the socket type from.
   * @return A {@link ZMTPSocketType}.
   * @throws ZMTPParsingException      If the socket type is malformed.
   * @throws IndexOutOfBoundsException If there is not enough readable bytes to read an entire
   *                                   socket type.
   */
  static ZMTPSocketType readSocketType(final ByteBuf in) throws ZMTPParsingException {
    return socketType((int) in.readByte());
  }

  /**
   * Translate a ZMTP/2.0 wire socket type representation to a {@link ZMTPSocketType}.
   *
   * @param socketType The ZMTP/2.0 wire socket type.
   * @return a {@link ZMTPSocketType}
   * @throws ZMTPParsingException If the socket type is invalid.
   */
  static ZMTPSocketType socketType(final int socketType) throws ZMTPParsingException {
    switch (socketType) {
      case 0:
        return ZMTPSocketType.PAIR;
      case 1:
        return ZMTPSocketType.PUB;
      case 2:
        return ZMTPSocketType.SUB;
      case 3:
        return ZMTPSocketType.REQ;
      case 4:
        return ZMTPSocketType.REP;
      case 5:
        return ZMTPSocketType.DEALER;
      case 6:
        return ZMTPSocketType.ROUTER;
      case 7:
        return ZMTPSocketType.PULL;
      case 8:
        return ZMTPSocketType.PUSH;
      default:
        throw new ZMTPParsingException("Invalid socket type: " + socketType);
    }
  }

  /**
   * Write a ZMTP/2.0 socket type.
   *
   * @param out        The buffer to write the socket type to.
   * @param socketType The socket type to write.
   */
  static void writeSocketType(final ByteBuf out, final ZMTPSocketType socketType) {
    out.writeByte(socketType(socketType));
  }

  /**
   * Translate a {@link ZMTPSocketType} to a ZMTP/2.0 wire representation.
   *
   * @param socketType The {@link ZMTPSocketType}.
   * @return The ZMTP/2.0 wire representation.
   */
  static int socketType(final ZMTPSocketType socketType) {
    switch (socketType) {
      case PAIR:
        return 0;
      case PUB:
        return 1;
      case SUB:
        return 2;
      case REQ:
        return 3;
      case REP:
        return 4;
      case DEALER:
        return 5;
      case ROUTER:
        return 6;
      case PULL:
        return 7;
      case PUSH:
        return 8;
      default:
        throw new IllegalArgumentException("Unknown socket type: " + socketType);
    }
  }

  /**
   * Read enough bytes from buffer to deduce the remote protocol version in a backwards compatible
   * ZMTP handshake.
   *
   * @param in the buffer of data to determine version from.
   * @return The detected {@link ZMTPVersion}.
   * @throws IndexOutOfBoundsException if there is not enough data available in buffer.
   */
  static ZMTPVersion detectProtocolVersion(final ByteBuf in) {
    if (in.readByte() != (byte) 0xff) {
      return ZMTPVersion.ZMTP10;
    }
    in.skipBytes(8);
    if ((in.readByte() & 0x01) == 0) {
      return ZMTPVersion.ZMTP10;
    }
    return ZMTPVersion.ZMTP20;
  }

  /**
   * Write a ZMTP/2.0 greeting.
   *
   * @param out        The buffer to write the greeting to.
   * @param socketType The socket type.
   * @param identity   The socket identity.
   */
  static void writeGreeting(final ByteBuf out, final ZMTPSocketType socketType,
                            final ByteBuffer identity) {
    writeSignature(out);
    writeGreetingBody(out, socketType, identity);
  }

  /**
   * Write a ZMTP/2.0 greeting signature.
   *
   * @param out The buffer to write the signature to.
   */
  static void writeSignature(final ByteBuf out) {
    out.writeByte(0xff);
    out.writeLong(0x00);
    out.writeByte(0x7f);
  }

  /**
   * Write a ZMTP/2.0 greeting body.
   *
   * @param out        The buffer to write the greeting body to.
   * @param socketType The socket type.
   * @param identity   The socket identity.
   */
  static void writeGreetingBody(final ByteBuf out, final ZMTPSocketType socketType,
                                final ByteBuffer identity) {
    out.writeByte(0x01);
    // socket-type
    writeSocketType(out, socketType);
    // identity
    // the final-short flag octet
    out.writeByte(0x00);
    out.writeByte(identity.remaining());
    out.writeBytes(identity.duplicate());
  }

  /**
   * Write a backwards compatible ZMTP/2.0 greeting signature.
   *
   * @param out      The buffer to write the signature to.
   * @param identity The socket identity.
   */
  static void writeCompatSignature(final ByteBuf out, final ByteBuffer identity) {
    out.writeByte(0xFF);
    out.writeLong(identity.remaining() + 1);
    out.writeByte(0x7f);
  }

  static class ZMTP20Header implements Header {

    int maxLength;
    int length;
    boolean more;

    @Override
    public void set(final int maxLength, final int length, final boolean more) {
      this.maxLength = maxLength;
      this.length = length;
      this.more = more;
    }

    @Override
    public void write(final ByteBuf out) {
      final byte flags = more ? MORE_FLAG : FINAL_FLAG;
      if (maxLength < 256) {
        out.writeByte(flags);
        out.writeByte((byte) length);
      } else {
        out.writeByte(flags | LONG_FLAG);
        out.writeLong(length);
      }
    }

    @Override
    public boolean read(final ByteBuf in) {
      if (in.readableBytes() < 2) {
        return false;
      }
      int flags = in.readByte();
      more = (flags & MORE_FLAG) == MORE_FLAG;
      if ((flags & LONG_FLAG) != LONG_FLAG) {
        length = in.readByte() & 0xff;
        return true;
      }
      if (in.readableBytes() < 8) {
        return false;
      }
      final long len = in.readLong();
      length = (int) len;
      return true;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public boolean more() {
      return more;
    }
  }

  /**
   * The ZMTP/2.0 greeting contents.
   */
  static class Greeting {

    private final int revision;
    private final ZMTPSocketType socketType;
    private final ByteBuffer identity;

    Greeting(final int revision, final ZMTPSocketType socketType, final ByteBuffer identity) {
      this.revision = revision;
      this.socketType = checkNotNull(socketType, "socketType");
      this.identity = checkNotNull(identity, "identity");
    }

    int revision() {
      return revision;
    }

    ZMTPSocketType socketType() {
      return socketType;
    }

    ByteBuffer identity() {
      return identity.asReadOnlyBuffer();
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }

      final Greeting that = (Greeting) o;

      if (revision != that.revision) { return false; }
      if (socketType != that.socketType) { return false; }
      return !(identity != null ? !identity.equals(that.identity) : that.identity != null);

    }

    @Override
    public int hashCode() {
      int result = revision;
      result = 31 * result + (socketType != null ? socketType.hashCode() : 0);
      result = 31 * result + (identity != null ? identity.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "Greeting{" +
             "revision=" + revision +
             ", socketType=" + socketType +
             ", identity=" + identity +
             '}';
    }
  }
}
