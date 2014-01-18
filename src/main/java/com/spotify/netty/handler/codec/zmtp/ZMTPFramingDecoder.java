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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.ZMTPUtils.FINAL_FLAG;

/**
 * Netty FrameDecoder for zmtp protocol
 *
 * Decodes ZMTP frames into a ZMTPMessage - will return a ZMTPMessage as a message event
 */
public class ZMTPFramingDecoder extends ByteToMessageDecoder {

  private final ZMTPMessageParser parser;
  private final ZMTPSession session;

  public ZMTPFramingDecoder(final ZMTPSession session) {
    this.session = session;
    this.parser = new ZMTPMessageParser(session.isEnveloped(), session.getSizeLimit());
  }

  /**
   * Sends my local identity
   */
  private ChannelFuture sendIdentity(final Channel channel) {
    final ByteBuf msg;

    if (session.useLocalIdentity()) {
      // send session current identity
      msg = Unpooled.buffer(2 + session.getLocalIdentity().length);

      ZMTPUtils.encodeLength(1 + session.getLocalIdentity().length, msg);
      msg.writeByte(FINAL_FLAG);
      msg.writeBytes(session.getLocalIdentity());
    } else {
      msg = Unpooled.buffer(2);
      // Anonymous identity
      msg.writeByte(1);
      msg.writeByte(FINAL_FLAG);
    }

    // Send identity message
    return channel.writeAndFlush(msg);
  }

  /**
   * Parses the remote zmtp identity received
   */
  private boolean handleRemoteIdentity(final ByteBuf buffer) throws ZMTPException {
    buffer.markReaderIndex();

    final long len = ZMTPUtils.decodeLength(buffer);
    if (len > 256) {
      // spec says the ident string can be up to 255 chars
      throw new ZMTPException("Remote identity longer than the allowed 255 octets");
    }

    // Bail out if there's not enough data
    if (len == -1 || buffer.readableBytes() < len) {
      buffer.resetReaderIndex();
      return false;
    }

    // Discard flag byte (allow for ZMTP 1.0 detection)
    buffer.readByte();

    if (len == 1) {
      // Anonymous identity
      session.setRemoteIdentity(null);
    } else {
      // Read identity from remote
      final byte[] identity = new byte[(int) len - 1];
      buffer.readBytes(identity);

      // Anonymous identity
      session.setRemoteIdentity(identity);
    }

    return true;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    // Store channel in the session
    this.session.setChannel(ctx.channel());

    sendIdentity(ctx.channel()).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          ctx.fireChannelActive();
        } else {
          throw new ZMTPException("handshake failed", future.cause());
        }
      }
    });
  }

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		if (in.readableBytes() < 2) {
			return;
		}
		if (session.getRemoteIdentity() == null) {
			// Should be first packet received from host
			if (!handleRemoteIdentity(in)) {
				return;
			}
		}

		ZMTPParsedMessage msg = parser.parse(in);
		if (msg == null) {
			return;
		}
		out.add(new ZMTPIncomingMessage(session, msg.getMessage(), msg.isTruncated(), msg.getByteSize()));
	}
}
