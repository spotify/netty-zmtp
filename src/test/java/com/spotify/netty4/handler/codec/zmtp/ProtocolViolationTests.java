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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ProtocolViolationTests {

  private Channel serverChannel;
  private InetSocketAddress serverAddress;

  private final String identity = "identity";
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup group;

  @ChannelHandler.Sharable
  private static class MockHandler extends ChannelInboundHandlerAdapter {

    private volatile boolean activeCalled;
    private volatile boolean readCalled;
    private volatile boolean handshaked;

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      activeCalled = true;
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
      if (evt instanceof ZMTPSession) {
        handshaked = true;
      }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      ReferenceCountUtil.releaseLater(msg);
      readCalled = true;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
        throws Exception {
      // ignore
    }
  }

  private final MockHandler mockHandler = new MockHandler();

  @Before
  public void setup() {
    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.channel(NioServerSocketChannel.class);
    bossGroup = new NioEventLoopGroup(1);
    group = new NioEventLoopGroup();
    serverBootstrap.group(bossGroup, group);
    serverBootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(
            ZMTPCodec.builder()
                .protocol(ZMTP20)
                .socketType(ROUTER)
                .localIdentity(identity)
                .build(),
            mockHandler);
      }
    });

    serverChannel = serverBootstrap.bind(new InetSocketAddress("localhost", 0))
        .awaitUninterruptibly().channel();
    serverAddress = (InetSocketAddress) serverChannel.localAddress();
  }

  @After
  public void teardown() {
    if (serverChannel != null) {
      serverChannel.close();
      serverChannel.closeFuture().awaitUninterruptibly();
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully().awaitUninterruptibly();
    }
    if (group != null) {
      group.shutdownGracefully().awaitUninterruptibly();
    }
  }

  @Test
  public void testBadConnection() throws Exception {
    for (int i = 0; i < 32; i++) {
      testConnect(i);
    }
  }

  private void testConnect(final int payloadSize) throws Exception {
    final Bootstrap b = new Bootstrap();
    b.group(new NioEventLoopGroup());
    b.channel(NioSocketChannel.class);
    b.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
      }
    });

    final Channel channel = b.connect(serverAddress).awaitUninterruptibly().channel();

    final StringBuilder payload = new StringBuilder();
    for (int i = 0; i < payloadSize; i++) {
      payload.append('0');
    }
    channel.writeAndFlush(Unpooled.copiedBuffer(payload.toString().getBytes()));

    Thread.sleep(100);

    assertTrue(mockHandler.activeCalled);
    assertFalse(mockHandler.handshaked);
    assertFalse(mockHandler.readCalled);
  }
}
