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

import com.google.common.util.concurrent.SettableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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

import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;

@RunWith(Theories.class)
public class ProtocolViolationTests {

  private Channel serverChannel;
  private InetSocketAddress serverAddress;

  private final String identity = "identity";
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup group;

  @ChannelHandler.Sharable
  private static class MockHandler extends ChannelInboundHandlerAdapter {

    private SettableFuture<Void> active = SettableFuture.create();
    private SettableFuture<Throwable> exception = SettableFuture.create();
    private SettableFuture<Void> inactive = SettableFuture.create();

    private volatile boolean handshaked;
    private volatile boolean read;

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      active.set(null);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      inactive.set(null);
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
      if (evt instanceof ZMTPHandshakeSuccess) {
        handshaked = true;
      }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      ReferenceCountUtil.release(msg);
      read = true;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
        throws Exception {
      exception.set(cause);
      ctx.close();
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
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (group != null) {
      group.shutdownGracefully();
    }
  }

  @Theory
  public void protocolErrorsCauseException(
      @TestedOn(ints = {16, 17, 27, 32, 48, 53}) final int payloadSize) throws Exception {
    final Bootstrap b = new Bootstrap();
    b.group(new NioEventLoopGroup());
    b.channel(NioSocketChannel.class);
    b.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new MockHandler());
      }
    });

    final Channel channel = b.connect(serverAddress).awaitUninterruptibly().channel();

    final ByteBuf payload = Unpooled.buffer(payloadSize);
    for (int i = 0; i < payloadSize; i++) {
      payload.writeByte(0);
    }
    channel.writeAndFlush(payload);

    mockHandler.active.get(5, SECONDS);
    mockHandler.exception.get(5, SECONDS);
    mockHandler.inactive.get(5, SECONDS);
    assertFalse(mockHandler.handshaked);
    assertFalse(mockHandler.read);
  }
}
