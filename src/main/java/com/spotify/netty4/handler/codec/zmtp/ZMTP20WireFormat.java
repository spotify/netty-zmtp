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

import io.netty.buffer.ByteBuf;

class ZMTP20WireFormat implements ZMTPWireFormat {

  static final byte FINAL_FLAG = 0x0;
  static final byte LONG_FLAG = 0x02;
  static final byte MORE_FLAG = 0x1;

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

  @Override
  public Header header() {
    return new ZMTP20Header();
  }

  @Override
  public int frameLength(final int content) {
    if (content < 256) {
      return 1 + 1 + content;
    } else {
      return 1 + 8 + content;
    }
  }
}
