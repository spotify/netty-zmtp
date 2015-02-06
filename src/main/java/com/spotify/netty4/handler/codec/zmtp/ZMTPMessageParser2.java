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

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.MORE_FLAG;

/**
 * Decodes ZMTP messages from a channel buffer, reading and accumulating frame by frame, keeping
 * state and updating buffer reader indices as necessary.
 */
public class ZMTPMessageParser2 {

  private static final byte LONG_FLAG = 0x02;

  private final int version;

  private final ZMTPMessageDecoder2 decoder;

  private boolean hasMore;
  private int length;
  private int remaining;
  private boolean headerParsed;


  public ZMTPMessageParser2(final int version, final ZMTPMessageDecoder2 decoder) {
    this.version = version;
    this.decoder = decoder;
  }

  /**
   * Parses as many whole frames from the buffer as possible, until the final frame is encountered.
   * If the message was completed, it returns the frames of the message. Otherwise it returns null
   * to indicate that more data is needed.
   *
   * @param buffer Buffer with data
   * @return The result from {@link ZMTPMessageDecoder2#finish} if the message was completely
   * parsed,
   * null otherwise.
   */
  public Object parse(final ByteBuf buffer) throws ZMTPMessageParsingException {
    while (buffer.isReadable()) {
      if (!headerParsed) {
        final int mark = buffer.readerIndex();
        headerParsed = parseZMTPHeader(buffer);
        if (!headerParsed) {
          // Wait for more data
          buffer.readerIndex(mark);
          return null;
        }
        decoder.frame(length, hasMore);
        remaining = length;
      }

      final int writerMark = buffer.writerIndex();
      final int size = Math.min(remaining, buffer.readableBytes());
      final int readerMark = buffer.readerIndex();
      buffer.writerIndex(readerMark + size);
      decoder.content(buffer);
      buffer.writerIndex(writerMark);
      final int read = buffer.readerIndex() - readerMark;
      remaining -= read;
      if (remaining > 0) {
        // Wait for more data
        return null;
      }
      headerParsed = false;
      if (!hasMore) {
        return decoder.finish();
      }
    }
    return null;
  }

  /**
   * Parse a ZMTP frame header.
   */
  private boolean parseZMTPHeader(final ByteBuf buffer) throws ZMTPMessageParsingException {
    return version == 1 ? parseZMTP1Header(buffer) : parseZMTP2Header(buffer);
  }

  /**
   * Parse a ZMTP/1.0 frame header.
   */
  private boolean parseZMTP1Header(final ByteBuf buffer) throws ZMTPMessageParsingException {
    final long len = ZMTPUtils.decodeZMTP1Length(buffer);

    if (len > Integer.MAX_VALUE) {
      throw new ZMTPMessageParsingException("Received too large frame: " + len);
    }

    if (len == -1) {
      return false;
    }

    if (len == 0) {
      throw new ZMTPMessageParsingException("Received frame with zero length");
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
  private boolean parseZMTP2Header(ByteBuf buffer) throws ZMTPMessageParsingException {
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
      throw new ZMTPMessageParsingException("Received too large frame: " + len);
    }
    length = (int) len;
    return true;
  }

  public static ZMTPMessageParser2 create(final int version, final ZMTPMessageDecoder2 decoder) {
    return new ZMTPMessageParser2(version, decoder);
  }
}
