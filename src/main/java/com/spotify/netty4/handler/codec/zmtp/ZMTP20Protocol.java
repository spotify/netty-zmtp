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

import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.detectProtocolVersion;
import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.readGreeting;
import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.readGreetingBody;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.UNKNOWN;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkArgument;

class ZMTP20Protocol implements ZMTPProtocol {

  @Override
  public ZMTPHandshaker handshaker(final ZMTPConfig config) {
    return new Handshaker(config.socketType(), config.interop(), config.localIdentity());
  }

  static class Handshaker implements ZMTPHandshaker {

    private final ZMTPSocketType socketType;
    private final ByteBuffer identity;
    private final boolean interop;

    private boolean splitHandshake;

    Handshaker(final ZMTPSocketType socketType, final boolean interop, final ByteBuffer identity) {
      checkArgument(socketType != null && socketType != UNKNOWN, "ZMTP/2.0 requires a socket type");
      this.socketType = socketType;
      this.identity = identity;
      this.interop = interop;
    }

    @Override
    public ByteBuf greeting() {
      final ByteBuf out = Unpooled.buffer();
      if (interop) {
        ZMTP20WireFormat.writeCompatSignature(out, identity);
      } else {
        ZMTP20WireFormat.writeGreeting(out, socketType, identity);
      }
      return out;
    }

    @Override
    public ZMTPHandshake handshake(final ByteBuf in, final ChannelHandlerContext ctx)
        throws ZMTPException {
      if (splitHandshake) {
        final ZMTPGreeting remoteGreeting = readGreetingBody(in);
        if (remoteGreeting.revision() < 1) {
          throw new ZMTPException("Bad ZMTP revision: " + remoteGreeting.revision());
        }
        return ZMTPHandshake.of(ZMTPVersion.ZMTP20, remoteGreeting);
      }

      if (interop) {
        final int mark = in.readerIndex();
        final ZMTPVersion version = detectProtocolVersion(in);
        switch (version) {
          case ZMTP10:
            in.readerIndex(mark);
            // when a ZMTP/1.0 peer is detected, just send the identity bytes. Together
            // with the compatibility signature it makes for a valid ZMTP/1.0 greeting.
            ctx.writeAndFlush(Unpooled.wrappedBuffer(identity));
            final ZMTPGreeting remoteGreeting = ZMTP10WireFormat.readGreeting(in);
            assert remoteGreeting != null;
            return ZMTPHandshake.of(version, remoteGreeting);
          case ZMTP20:
            splitHandshake = true;
            final ByteBuf out = Unpooled.buffer();
            ctx.writeAndFlush(ZMTP20WireFormat.writeGreetingBody(out, socketType, identity));
            return null;
          default:
            throw new ZMTPException("Unknown ZMTP version: " + version);
        }
      } else {
        final ZMTPGreeting greeting = readGreeting(in);
        return ZMTPHandshake.of(ZMTPVersion.ZMTP20, greeting);
      }
    }

  }

}
