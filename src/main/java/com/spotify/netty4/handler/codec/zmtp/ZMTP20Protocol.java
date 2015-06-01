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

import com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.Greeting;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.detectProtocolVersion;
import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.readGreeting;
import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.readGreetingBody;
import static com.spotify.netty4.handler.codec.zmtp.ZMTP20WireFormat.writeGreetingBody;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP20;

class ZMTP20Protocol implements ZMTPProtocol {

  @Override
  public ZMTPHandshaker handshaker(final ZMTPConfig config) {
    return new Handshaker(config.socketType(), config.localIdentity(), config.interop());
  }

  static class Handshaker implements ZMTPHandshaker {

    private final ZMTPSocketType socketType;
    private final ByteBuffer identity;
    private final boolean interop;

    private boolean splitHandshake;

    Handshaker(final ZMTPSocketType socketType, final ByteBuffer identity, final boolean interop) {
      this.socketType = checkNotNull(socketType, "ZMTP/2.0 requires a socket type");
      this.identity = checkNotNull(identity, "identity");
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
        final Greeting remoteGreeting = readGreetingBody(in);
        if (remoteGreeting.revision() < 1) {
          throw new ZMTPException("Bad ZMTP revision: " + remoteGreeting.revision());
        }
        return ZMTPHandshake.of(ZMTP20, remoteGreeting.identity(), remoteGreeting.socketType());
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
            final ByteBuffer remoteIdentity = ZMTP10WireFormat.readIdentity(in);
            assert remoteIdentity != null;
            return ZMTPHandshake.of(ZMTP10, remoteIdentity);
          case ZMTP20:
            splitHandshake = true;
            final ByteBuf out = Unpooled.buffer();
            writeGreetingBody(out, socketType, identity);
            ctx.writeAndFlush(out);
            return null;
          default:
            throw new ZMTPException("Unknown ZMTP version: " + version);
        }
      } else {
        final Greeting remoteGreeting = readGreeting(in);
        return ZMTPHandshake.of(ZMTP20, remoteGreeting.identity(), remoteGreeting.socketType());
      }
    }

  }

  @Override
  public String toString() {
    return "ZMTP/2.0";
  }
}
