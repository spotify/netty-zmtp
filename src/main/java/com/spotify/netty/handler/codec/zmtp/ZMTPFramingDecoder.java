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

package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * Netty FrameDecoder for zmtp protocol
 *
 * Decodes ZMTP frames into a ZMTPMessage - will return a ZMTPMessage as a message event
 */
class ZMTPFramingDecoder extends FrameDecoder {

  private final ZMTPMessageParser parser;
  private final ZMTPSession session;

  /**
   * Creates a new decoder
   */
  public ZMTPFramingDecoder(final ZMTPSession session) {
    this.parser = new ZMTPMessageParser(session.getSizeLimit(),
                                        session.getActualVersion());
    this.session = session;
  }

  /**
   * Responsible for decoding incoming data to zmtp frames
   */
  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
      throws Exception {

    // Parse incoming frames
    final ZMTPParsedMessage msg = parser.parse(buffer);
    if (msg == null) {
      return null;
    }

    return new ZMTPIncomingMessage(session, msg.getMessage(), msg.isTruncated(), msg.getByteSize());
  }
}
