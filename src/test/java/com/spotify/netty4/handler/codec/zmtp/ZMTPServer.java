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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static com.google.common.collect.Queues.newLinkedBlockingQueue;
import static io.netty.util.ReferenceCountUtil.retain;

/**
 * A simple ZMTP server for testing purposes.
 */
public class ZMTPServer implements Closeable, ZMTPSocket {

  private Channel serverChannel;
  private InetSocketAddress serverAddress;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup group;

  private final BlockingQueue<ZMTPIncomingMessage> incomingMessages = newLinkedBlockingQueue();
  private final SettableFuture<Channel> incomingChannel = SettableFuture.create();

  private final ZMTPCodec codec;
  private final InetSocketAddress bindAddress;

  public ZMTPServer(final ZMTPCodec codec) {
    this(codec, new InetSocketAddress("127.0.0.1", 0));
  }

  public ZMTPServer(final ZMTPCodec codec, final InetSocketAddress bindAddress) {
    this.codec = codec;
    this.bindAddress = bindAddress;
  }

  public InetSocketAddress address() {
    return serverAddress;
  }

  public ListenableFuture<Channel> incomingChannel() {
    return incomingChannel;
  }

  public BlockingQueue<ZMTPIncomingMessage> incomingMessages() {
    return incomingMessages;
  }

  public void start() {
    final ServerBootstrap b = new ServerBootstrap();
    bossGroup = new NioEventLoopGroup(1);
    group = new NioEventLoopGroup();

    b.channel(NioServerSocketChannel.class);
    b.group(bossGroup, group);
    b.childHandler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(codec, new Handler());
      }
    });

    serverChannel = b.bind(bindAddress).awaitUninterruptibly().channel();
    serverAddress = (InetSocketAddress) serverChannel.localAddress();
  }

  @Override
  public void close() {
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

  public String endpoint() {
    return "tcp://" + serverAddress.getHostString() + ":" + serverAddress.getPort();
  }

  @Override
  public void send(final ZMTPMessage request) throws InterruptedException {
    try {
      incomingChannel.get().writeAndFlush(request);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ZMTPIncomingMessage recv() throws InterruptedException {
    return incomingMessages.take();
  }

  private class Handler extends ChannelInboundHandlerAdapter {

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
        throws Exception {
      if (evt instanceof ZMTPSession) {
        incomingChannel.set(ctx.channel());
      }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg)
        throws Exception {
      incomingMessages.put((ZMTPIncomingMessage) retain(msg));
    }
  }
}
