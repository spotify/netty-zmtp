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

import org.zeromq.ZMQ;

import java.net.InetSocketAddress;
import java.util.List;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;

/**
 * Helper to from connections to a zmtp server via netty
 */
abstract class ZMTPTestConnector {

  private boolean receivedMessage = false;

  public abstract void preConnect(ZMQ.Socket socket);

  public abstract void afterConnect(ZMQ.Socket socket, ChannelFuture future);

  public abstract boolean onMessage(ZMTPIncomingMessage msg);

  public boolean connectAndReceive(final int serverType) {
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket serverSocket = context.socket(serverType);

    preConnect(serverSocket);

    final int port = serverSocket.bindToRandomPort("tcp://127.0.0.1");

    // Configure the client.
    final Bootstrap bootstrap = new Bootstrap();
    final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    bootstrap.group(eventLoopGroup);
    bootstrap.channel(NioSocketChannel.class);

    // Set up the pipeline factory.
    bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(
            ZMTPCodec.builder()
                .socketType(DEALER)
                .localIdentity("client")
                .build(),
            new MessageToMessageDecoder<ZMTPIncomingMessage>() {
              @Override
              public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
                if (evt instanceof ZMTPSession) {
                  onConnect((ZMTPSession) evt);
                }
              }

              @Override
              protected void decode(final ChannelHandlerContext ctx, final ZMTPIncomingMessage msg,
                                    final List<Object> out)
                  throws Exception {
                if (onMessage(msg)) {
                  receivedMessage = true;
                  ctx.close();
                }
              }
            });
      }
    });

    // Start the connection attempt.
    final ChannelFuture future = bootstrap.connect(new InetSocketAddress("127.0.0.1", port));

    future.awaitUninterruptibly();

    afterConnect(serverSocket, future);

    // Wait until the connection is closed or the connection attempt fails.
    future.channel().closeFuture().awaitUninterruptibly();

    // Shut down thread pools to exit.
    eventLoopGroup.shutdownGracefully().awaitUninterruptibly();

    serverSocket.close();
    context.term();

    return receivedMessage;
  }

  protected abstract void onConnect(final ZMTPSession session);
}
