/*
 * Copyright (c) 2012-2013 Spotify AB
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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import static java.lang.Math.min;

/**
 * Netty ZMTP decoder.
 */
class ZMTPFramingDecoder extends ByteToMessageDecoder {

  private final ZMTPDecoder decoder;
  private final ZMTPWireFormat.Header header;

  private long remaining;
  private boolean headerParsed;

  public ZMTPFramingDecoder(final ZMTPWireFormat wireFormat, final ZMTPDecoder decoder) {
    this.header = wireFormat.header();
    this.decoder = decoder;
  }

  @Override
  protected void handlerRemoved0(final ChannelHandlerContext ctx) {
    decoder.close();
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws ZMTPParsingException {
    while (in.isReadable()) {
      if (!headerParsed) {
        final int mark = in.readerIndex();
        headerParsed = header.read(in);
        if (!headerParsed) {
          // Wait for more data
          in.readerIndex(mark);
          return;
        }
        decoder.header(ctx, header.length(), header.more(), out);
        remaining = header.length();
      }

      final int writerMark = in.writerIndex();
      final int n = (int) min(remaining, in.readableBytes());
      final int readerMark = in.readerIndex();
      in.writerIndex(readerMark + n);
      decoder.content(ctx, in, out);
      in.writerIndex(writerMark);
      final int read = in.readerIndex() - readerMark;
      remaining -= read;
      if (remaining > 0) {
        // Wait for more data
        return;
      }
      if (!header.more()) {
        decoder.finish(ctx, out);
      }
      headerParsed = false;
    }
  }
}
