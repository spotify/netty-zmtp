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

class ZMTP10WireFormat implements ZMTPWireFormat {

  private static final byte FINAL_FLAG = 0x0;
  private static final byte MORE_FLAG = 0x1;

  /**
   * Read the remote identity octets from a ZMTP/1.0 greeting.
   */
  static ByteBuffer readIdentity(final ByteBuf buffer) throws ZMTPParsingException {
    final long length = readLength(buffer);
    if (length == -1) {
      return null;
    }
    final long identityLength = length - 1;
    if (identityLength < 0 || identityLength > 255) {
      throw new ZMTPParsingException("Bad remote identity length: " + length);
    }

    // skip the flags byte
    buffer.skipBytes(1);

    final byte[] identity = new byte[(int) identityLength];
    buffer.readBytes(identity);
    return ByteBuffer.wrap(identity);
  }

  /**
   * Read a ZMTP/1.0 frame length.
   */
  static long readLength(final ByteBuf in) {
    if (in.readableBytes() < 1) {
      return -1;
    }
    long size = in.readByte() & 0xFF;
    if (size == 0xFF) {
      if (in.readableBytes() < 8) {
        return -1;
      }
      size = in.readLong();
    }

    return size;
  }

  /**
   * Write a ZMTP/1.0 frame length.
   *
   * @param length The length.
   * @param out    Target buffer.
   */
  static void writeLength(final long length, final ByteBuf out) {
    writeLength(out, length, length);
  }

  /**
   * Write a ZMTP/1.0 frame length.
   *
   * @param out       Target buffer.
   * @param maxLength The maximum length of the field.
   * @param length    The length.
   */
  static void writeLength(final ByteBuf out, final long maxLength, final long length) {
    if (maxLength < 255) {
      out.writeByte((byte) length);
    } else {
      out.writeByte(0xFF);
      out.writeLong(length);
    }
  }

  /**
   * Write a ZMTP/1.0 greeting.
   *
   * @param out      Target buffer.
   * @param identity Socket identity.
   */
  static void writeGreeting(final ByteBuf out, final ByteBuffer identity) {
    writeLength(identity.remaining() + 1, out);
    out.writeByte(0x00);
    out.writeBytes(identity.duplicate());
  }

  @Override
  public Header header() {
    return new ZMTP10Header();
  }

  @Override
  public int frameLength(final int content) {
    if (content + 1 < 255) {
      return 1 + 1 + content;
    } else {
      return 1 + 8 + 1 + content;
    }
  }

  static class ZMTP10Header implements Header {

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
      writeLength(out, maxLength + 1, length + 1);
      out.writeByte(more ? MORE_FLAG : FINAL_FLAG);
    }

    @Override
    public boolean read(final ByteBuf in) throws ZMTPParsingException {
      final long len = readLength(in);
      if (len == -1) {
        // Wait for more data
        return false;
      }

      if (len == 0) {
        throw new ZMTPParsingException("Received frame with zero length");
      }

      if (in.readableBytes() < 1) {
        // Wait for more data
        return false;
      }

      length = (int) len - 1;
      more = (in.readByte() & MORE_FLAG) == MORE_FLAG;

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
}
