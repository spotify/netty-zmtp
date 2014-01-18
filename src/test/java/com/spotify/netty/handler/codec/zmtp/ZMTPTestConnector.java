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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.jeromq.ZMQ;

import java.net.InetSocketAddress;
import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.ZMTPConnectionType.Addressed;

/**
 * Helper to create connections to a zmtp server via netty
 */
public abstract class ZMTPTestConnector {
//
//  public ZMQ.Context context;
//  public ZMQ.Socket serverSocket;
//
//  boolean receivedMessage = false;
//
//  public abstract void preConnect(ZMQ.Socket socket);
//
//  public abstract void afterConnect(ZMQ.Socket socket, ChannelFuture future);
//
//  public abstract boolean onMessage(ZMTPIncomingMessage msg);
//
//  public boolean connectAndReceive(final String ip, final int port, final int serverType) {
//    context = ZMQ.context(1);
//    serverSocket = context.socket(serverType);
//
//    preConnect(serverSocket);
//
//    serverSocket.bind("tcp://" + ip + ":" + port);
//
//    // Configure the client.
//    final Bootstrap bootstrap = new Bootstrap();
//
//    // Set up the pipeline factory.
//
////	  bootstrap.handler(new ByteToMessageDecoder() {
////		  @Override
////		  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
////			  if (onMessage()) {
////				  receivedMessage = true;
////				  ctx.channel().close();
////			  }
////		  }
////	  });
//    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
//      public ChannelPipeline getPipeline() throws Exception {
//        final ZMTPSession session = new ZMTPSession(Addressed);
//        return Channels.pipeline(
//            new ZMTPFramingDecoder(session),
//            new OneToOneDecoder() {
//              @Override
//              protected Object decode(final ChannelHandlerContext ctx, final Channel channel,
//                                      final Object msg) throws Exception {
//                if (onMessage((ZMTPIncomingMessage) msg)) {
//                  receivedMessage = true;
//                  channel.close();
//                }
//			   ChannelBuffers.dynamicBuffer();
//                return null;
//              }
//            });
//      }
//    });
//
//    // Start the connection attempt.
//    final ChannelFuture future = bootstrap.connect(new InetSocketAddress(ip, port));
//
//    future.awaitUninterruptibly();
//
//    afterConnect(serverSocket, future);
//
//    // Wait until the connection is closed or the connection attempt fails.
//    future.channel().closeFuture().awaitUninterruptibly();
//
//    // Shut down thread pools to exit.
////    bootstrap.releaseExternalResources();
//
//
//    serverSocket.close();
//    context.term();
//
//    return receivedMessage;
//  }
}
