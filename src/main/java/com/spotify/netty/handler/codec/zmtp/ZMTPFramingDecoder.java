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
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import static com.spotify.netty.handler.codec.zmtp.ZMTPUtils.FINAL_FLAG;

/**
 * Netty FrameDecoder for zmtp protocol
 *
 * Decodes ZMTP frames into a ZMTPMessage - will return a ZMTPMessage as a message event
 */
public class ZMTPFramingDecoder extends FrameDecoder {

  private final ZMTPMessageParser parser;
  private final ZMTPSession session;

  private ChannelFuture handshakeFuture;

  public ZMTPFramingDecoder(final ZMTPSession session) {
    this.session = session;
    this.parser = new ZMTPMessageParser(session.isEnveloped(), session.getSizeLimit());
  }

  /**
   * Sends my local identity
   */
  private void sendIdentity(final Channel channel) {
    final ChannelBuffer msg;

    if (session.useLocalIdentity()) {
      // send session current identity
      msg = ChannelBuffers.dynamicBuffer(2 + session.getLocalIdentity().length);

      ZMTPUtils.encodeLength(1 + session.getLocalIdentity().length, msg);
      msg.writeByte(FINAL_FLAG);
      msg.writeBytes(session.getLocalIdentity());
    } else {
      msg = ChannelBuffers.dynamicBuffer(2);
      // Anonymous identity
      msg.writeByte(1);
      msg.writeByte(FINAL_FLAG);
    }

    // Send identity message
    channel.write(msg);
  }

  /**
   * Parses the remote zmtp identity received
   */
  private boolean handleRemoteIdentity(final ChannelBuffer buffer) throws ZMTPException {
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

    handshakeFuture.setSuccess();

    return true;
  }

  /**
   * Resposible for decoding incomming data to zmtp frames
   */
  @Override
  protected Object decode(
      final ChannelHandlerContext ctx, final Channel channel, final ChannelBuffer buffer)
      throws Exception {
    if (buffer.readableBytes() < 2) {
      return null;
    }

    if (session.getRemoteIdentity() == null) {
      // Should be first packet received from host
      if (!handleRemoteIdentity(buffer)) {
        return null;
      }
    }

    // Parse incoming frames
    final ZMTPParsedMessage message = parser.parse(buffer);
    if (message == null) {
      return null;
    }

    return new ZMTPIncomingMessage(session, message.getMessage(), message.isTruncated());
  }

  @Override
  public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e)
      throws Exception {
    // Store channel in the session
    this.session.setChannel(e.getChannel());

    handshake(ctx.getChannel()).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          ctx.sendUpstream(e);
        } else {
          throw new ZMTPException("handshake failed", future.getCause());
        }
      }
    });
  }

  private ChannelFuture handshake(final Channel channel) {
    handshakeFuture = Channels.future(channel);

    // Send our identity
    sendIdentity(channel);

    return handshakeFuture;
  }
}
