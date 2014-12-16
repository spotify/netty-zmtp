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

/**
 * Helper to create connections to a zmtp server via netty
 */
public abstract class ZMTPTestConnector {

  public ZMQ.Context context;
  public ZMQ.Socket serverSocket;

  boolean receivedMessage = false;

  public abstract void preConnect(ZMQ.Socket socket);

  public abstract void afterConnect(ZMQ.Socket socket, ChannelFuture future);

  public abstract boolean onMessage(ZMTPIncomingMessage msg);

  public boolean connectAndReceive(final String ip, final int port, final int serverType) {
    context = ZMQ.context(1);
    serverSocket = context.socket(serverType);

    preConnect(serverSocket);

    serverSocket.bind("tcp://" + ip + ":" + port);

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
            new ZMTP10Handshaker(new ZMTPSession(ZMTPConnectionType.ADDRESSED, "client".getBytes())),
            new MessageToMessageDecoder<ZMTPIncomingMessage>() {
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
    final ChannelFuture future = bootstrap.connect(new InetSocketAddress(ip, port));

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
}
