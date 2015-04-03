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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

class ZMTP10Protocol implements ZMTPProtocol {

  @Override
  public ZMTPHandshaker handshaker(final ZMTPConfig config) {
    return new Handshaker(config.localIdentity());
  }

  static class Handshaker implements ZMTPHandshaker {

    private final ByteBuffer localIdentity;

    Handshaker(final ByteBuffer localIdentity) {
      this.localIdentity = localIdentity;
    }

    @Override
    public ByteBuf greeting() {
      final ByteBuf out = Unpooled.buffer();
      return ZMTP10WireFormat.writeGreeting(out, localIdentity);
    }

    @Override
    public ZMTPHandshake handshake(final ByteBuf in, final ChannelHandlerContext ctx)
        throws ZMTPException {
      final ZMTPGreeting remoteGreeting = ZMTP10WireFormat.readGreeting(in);
      assert remoteGreeting != null;
      return new ZMTPHandshake(ZMTPVersion.ZMTP10, remoteGreeting);
    }
  }
}
