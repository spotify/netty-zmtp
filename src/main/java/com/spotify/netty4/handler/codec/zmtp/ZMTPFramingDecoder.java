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

/**
 * Netty FrameDecoder for zmtp protocol
 *
 * Decodes ZMTP frames into a ZMTPMessage - will return a ZMTPMessage as a message event
 */
class ZMTPFramingDecoder extends ByteToMessageDecoder {

  private final ZMTPMessageParser parser;
  private final ZMTPSession session;

  /**
   * Creates a new decoder
   */
  public ZMTPFramingDecoder(final ZMTPSession session) {
    this.parser = new ZMTPMessageParser(session.isEnveloped(), session.sizeLimit(),
                                        session.actualVersion());
    this.session = session;
  }

  /**
   * Responsible for decoding incoming data to zmtp frames
   */
  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {

    final ZMTPParsedMessage msg = parser.parse(in);
    if (msg == null) {
      return;
    }

    out.add(new ZMTPIncomingMessage(msg.message(), msg.isTruncated(), msg.byteSize()));
  }
}
