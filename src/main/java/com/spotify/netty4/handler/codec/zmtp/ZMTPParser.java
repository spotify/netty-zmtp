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

import static com.spotify.netty4.handler.codec.zmtp.ZMTPWireFormats.wireFormat;
import static java.lang.Math.min;

/**
 * Streaming ZMTP parser. Parses headers and calls a {@link ZMTPDecoder} for consuming the actual
 * payload.
 */
class ZMTPParser {

  private final ZMTPDecoder decoder;
  private final ZMTPWireFormat.Header header;

  private long remaining;
  private boolean headerParsed;

  ZMTPParser(final ZMTPWireFormat wireFormat, final ZMTPDecoder decoder) {
    this.header = wireFormat.header();
    this.decoder = decoder;
  }

  /**
   * Parse headers and call a {@link ZMTPDecoder} to produce some decoded output.
   *
   * @param buffer {@link ByteBuf} data to decode
   * @param out    {@link List} to which decoded messages should be added
   */
  void parse(final ByteBuf buffer, final List<Object> out)
      throws ZMTPParsingException {
    while (buffer.isReadable()) {
      if (!headerParsed) {
        final int mark = buffer.readerIndex();
        headerParsed = header.read(buffer);
        if (!headerParsed) {
          // Wait for more data
          buffer.readerIndex(mark);
          return;
        }
        decoder.header(header.length(), header.more(), out);
        remaining = header.length();
      }

      final int writerMark = buffer.writerIndex();
      final int n = (int) min(remaining, buffer.readableBytes());
      final int readerMark = buffer.readerIndex();
      buffer.writerIndex(readerMark + n);
      decoder.content(buffer, out);
      buffer.writerIndex(writerMark);
      final int read = buffer.readerIndex() - readerMark;
      remaining -= read;
      if (remaining > 0) {
        // Wait for more data
        return;
      }
      if (!header.more()) {
        decoder.finish(out);
      }
      headerParsed = false;
    }
  }

  static ZMTPParser create(final ZMTPVersion version, final ZMTPDecoder decoder) {
    return new ZMTPParser(wireFormat(version), decoder);
  }
}
