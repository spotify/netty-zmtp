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

import java.util.List;

import io.netty.buffer.ByteBuf;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.MORE_FLAG;

/**
 * Streaming ZMTP parser. Parses headers and calls a {@link ZMTPDecoder} for consuming the actual
 * payload.
 */
public class ZMTPParser {

  private static final byte LONG_FLAG = 0x02;

  private final int version;

  private final ZMTPDecoder decoder;

  private boolean hasMore;
  private int length;
  private int remaining;
  private boolean headerParsed;


  ZMTPParser(final int version, final ZMTPDecoder decoder) {
    this.version = version;
    this.decoder = decoder;
  }

  /**
   * Parse headers and call a {@link ZMTPDecoder} to produce some decoded output.
   *
   * @param buffer {@link ByteBuf} data to decode
   * @param out    {@link List} to which decoded messages should be added
   */
  public void parse(final ByteBuf buffer, final List<Object> out)
      throws ZMTPParsingException {
    while (buffer.isReadable()) {
      if (!headerParsed) {
        final int mark = buffer.readerIndex();
        headerParsed = parseZMTPHeader(buffer);
        if (!headerParsed) {
          // Wait for more data
          buffer.readerIndex(mark);
          return;
        }
        decoder.header(length, hasMore, out);
        remaining = length;
      }

      final int writerMark = buffer.writerIndex();
      final int size = Math.min(remaining, buffer.readableBytes());
      final int readerMark = buffer.readerIndex();
      buffer.writerIndex(readerMark + size);
      decoder.content(buffer, out);
      buffer.writerIndex(writerMark);
      final int read = buffer.readerIndex() - readerMark;
      remaining -= read;
      if (remaining > 0) {
        // Wait for more data
        return;
      }
      if (!hasMore) {
        decoder.finish(out);
      }
      headerParsed = false;
    }
  }

  /**
   * Parse a ZMTP frame header.
   */
  private boolean parseZMTPHeader(final ByteBuf buffer) throws ZMTPParsingException {
    return version == 1 ? parseZMTP1Header(buffer) : parseZMTP2Header(buffer);
  }

  /**
   * Parse a ZMTP/1.0 frame header.
   */
  private boolean parseZMTP1Header(final ByteBuf buffer) throws ZMTPParsingException {
    final long len = ZMTPUtils.decodeZMTP1Length(buffer);

    if (len > Integer.MAX_VALUE) {
      throw new ZMTPParsingException("Received too large frame: " + len);
    }

    if (len == -1) {
      return false;
    }

    if (len == 0) {
      throw new ZMTPParsingException("Received frame with zero length");
    }

    // Read if we have more frames from flag byte
    if (buffer.readableBytes() < 1) {
      // Wait for more data to decode
      return false;
    }

    length = (int) len - 1;
    hasMore = (buffer.readByte() & MORE_FLAG) == MORE_FLAG;

    return true;
  }

  /**
   * Parse a ZMTP/2.0 frame header.
   */
  private boolean parseZMTP2Header(ByteBuf buffer) throws ZMTPParsingException {
    if (buffer.readableBytes() < 2) {
      return false;
    }
    int flags = buffer.readByte();
    hasMore = (flags & MORE_FLAG) == MORE_FLAG;
    if ((flags & LONG_FLAG) != LONG_FLAG) {
      length = buffer.readByte() & 0xff;
      return true;
    }
    if (buffer.readableBytes() < 8) {
      return false;
    }
    final long len = buffer.readLong();
    if (len > Integer.MAX_VALUE) {
      throw new ZMTPParsingException("Received too large frame: " + len);
    }
    length = (int) len;
    return true;
  }

  public static ZMTPParser create(final int version, final ZMTPDecoder decoder) {
    return new ZMTPParser(version, decoder);
  }
}
