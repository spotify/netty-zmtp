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

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ProtocolViolationTests {

  private ServerBootstrap serverBootstrap;
  private Channel serverChannel;
  private InetSocketAddress serverAddress;

  private String identity = "identity";

  interface MockHandler {

    public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e);

    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e);
  }

  final MockHandler mockHandler = mock(MockHandler.class);

  @Before
  public void setup() {
    serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      Executor executor = new OrderedMemoryAwareThreadPoolExecutor(
          Runtime.getRuntime().availableProcessors(),
          1024 * 1024,
          128 * 1024 * 1024
      );

      public ChannelPipeline getPipeline() throws Exception {

        return Channels.pipeline(
            new ExecutionHandler(executor),
            new ZMTP10Codec(new ZMTPSession(ZMTPConnectionType.Addressed, identity.getBytes())),
            new SimpleChannelUpstreamHandler() {

              @Override
              public void channelConnected(final ChannelHandlerContext ctx,
                                           final ChannelStateEvent e) throws Exception {
                mockHandler.channelConnected(ctx, e);
              }

              @Override
              public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
                  throws Exception {
                mockHandler.messageReceived(ctx, e);
              }
            });
      }
    });

    serverChannel = serverBootstrap.bind(new InetSocketAddress("localhost", 0));
    serverAddress = (InetSocketAddress) serverChannel.getLocalAddress();
  }

  @After
  public void teardown() {
    if (serverChannel != null) {
      serverChannel.close();
      serverChannel.getCloseFuture().awaitUninterruptibly();
    }
    if (serverBootstrap != null) {
      serverBootstrap.releaseExternalResources();
    }
  }

  @Test
  public void testBadConnection() throws Exception {
    for (int i = 0; i < 32; i++) {
      testConnect(i);
    }
  }

  private void testConnect(final int payloadSize) throws InterruptedException {
    final ClientBootstrap clientBootstrap =
        new ClientBootstrap(new NioClientSocketChannelFactory());
    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new SimpleChannelUpstreamHandler());
      }
    });
    final ChannelFuture future = clientBootstrap.connect(serverAddress);
    future.awaitUninterruptibly();

    final Channel channel = future.getChannel();

    final StringBuilder payload = new StringBuilder();
    for (int i = 0; i < payloadSize; i++) {
      payload.append('0');
    }
    channel.write(ChannelBuffers.copiedBuffer(payload.toString().getBytes()));

    Thread.sleep(100);

    verify(mockHandler, never())
        .channelConnected(any(ChannelHandlerContext.class), any(ChannelStateEvent.class));
    verify(mockHandler, never())
        .messageReceived(any(ChannelHandlerContext.class), any(MessageEvent.class));
  }
}
