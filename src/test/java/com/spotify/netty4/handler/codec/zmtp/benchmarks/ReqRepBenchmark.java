/*
 * Copyright (c) 2012-2014 Spotify AB
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

package com.spotify.netty4.handler.codec.zmtp.benchmarks;

import com.google.common.base.Strings;

import com.spotify.netty4.handler.codec.zmtp.ZMTPCodec;
import com.spotify.netty4.handler.codec.zmtp.ZMTPHandshakeSuccess;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessage;
import com.spotify.netty4.util.BatchFlusher;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;

public class ReqRepBenchmark {

  private static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);

  public static void main(final String... args) throws InterruptedException {
    final ProgressMeter meter = new ProgressMeter("requests");

    // Codecs

    // Server
    final ServerBootstrap serverBootstrap = new ServerBootstrap()
        .group(new NioEventLoopGroup(1), new NioEventLoopGroup())
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childHandler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(final NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(ZMTPCodec.builder()
                                      .socketType(ROUTER)
                                      .build());
            ch.pipeline().addLast(new ServerHandler());
          }
        });
    final Channel server = serverBootstrap.bind(ANY_PORT).awaitUninterruptibly().channel();

    // Client
    final SocketAddress address = server.localAddress();
    final Bootstrap clientBootstrap = new Bootstrap()
        .group(new NioEventLoopGroup())
        .channel(NioSocketChannel.class)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .handler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(final NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(
                ZMTPCodec.builder()
                    .socketType(DEALER)
                    .build());
            ch.pipeline().addLast(new ClientHandler(meter));
          }
        });
    final Channel client = clientBootstrap.connect(address).awaitUninterruptibly().channel();

    // Run until client is closed
    client.closeFuture().await();
  }

  private static class ServerHandler extends ChannelInboundHandlerAdapter {

    private BatchFlusher flusher;

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
      this.flusher = new BatchFlusher(ctx.channel());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final ZMTPMessage message = (ZMTPMessage) msg;
      ctx.write(message);
      flusher.flush();
    }
  }

  private static class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final int CONCURRENCY = 1000;

    private static final ZMTPMessage REQUEST = ZMTPMessage.fromUTF8(
        "envelope1", "envelope2",
        "",
        Strings.repeat("d", 20),
        Strings.repeat("d", 40),
        Strings.repeat("d", 100));

    private final ProgressMeter meter;

    private BatchFlusher flusher;

    public ClientHandler(final ProgressMeter meter) {
      this.meter = meter;
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
      this.flusher = new BatchFlusher(ctx.channel());
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
        throws Exception {
      if (evt instanceof ZMTPHandshakeSuccess) {
        for (int i = 0; i < CONCURRENCY; i++) {
          ctx.write(req());
        }
        flusher.flush();
      }
    }

    private ZMTPMessage req() {
      return REQUEST.retain();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final ZMTPMessage message = (ZMTPMessage) msg;
      meter.inc();
      message.release();
      ctx.write(req());
      flusher.flush();
    }
  }
}
