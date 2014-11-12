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
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Netty encoder for ZMTP messages.
 */
class ZMTPFramingEncoder extends OneToOneEncoder {


  private final ZMTPSession session;

  public ZMTPFramingEncoder(final ZMTPSession session) {
    this.session = session;
  }

  @Override
  protected Object encode(final ChannelHandlerContext channelHandlerContext, final Channel channel,
                          final Object o)
      throws Exception {
    if (!(o instanceof ZMTPMessage)) {
      return o;
    }

    // TODO (dano): integrate with write batching to avoid buffer creation and reduce garbage

    final ZMTPMessage message = (ZMTPMessage) o;

    final int size = ZMTPUtils.messageSize(message, session.actualVersion());
    final ChannelBuffer buffer = ChannelBuffers.buffer(size);

    ZMTPUtils.writeMessage(message, buffer, session.actualVersion());

    return buffer;
  }

}
