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

import com.spotify.netty4.handler.codec.zmtp.ZMTP10Codec;
import com.spotify.netty4.handler.codec.zmtp.ZMTPFrame;
import com.spotify.netty4.handler.codec.zmtp.ZMTPIncomingMessage;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessage;
import com.spotify.netty4.handler.codec.zmtp.ZMTPSession;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.chmv8.ForkJoinPool;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPConnectionType.Addressed;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSession.DEFAULT_SIZE_LIMIT;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static java.util.Arrays.asList;

public class EndToEndBenchmark {

  private static final byte[] NO_IDENTITY = null;

  private static final boolean INTEROP_ON = true;
  private static final boolean INTEROP_OFF = false;

  private static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);
  public static final Thread.UncaughtExceptionHandler
      UNCAUGHT_EXCEPTION_HANDLER =
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(final Thread thread, final Throwable throwable) {
          throwable.printStackTrace();
        }
      };

  public static void main(final String... args) throws InterruptedException {
    final ProgressMeter meter = new ProgressMeter("requests");

    // Codecs
    final ZMTP10Codec serverCodec = new ZMTP10Codec(new ZMTPSession(
        Addressed, DEFAULT_SIZE_LIMIT, NO_IDENTITY, ROUTER));

    final ZMTP10Codec clientCodec = new ZMTP10Codec(new ZMTPSession(
        Addressed, DEFAULT_SIZE_LIMIT, NO_IDENTITY, DEALER));

    // Server
    final Executor serverExecutor = new ForkJoinPool(
        1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, UNCAUGHT_EXCEPTION_HANDLER, true);
    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(new NioEventLoopGroup(1), new NioEventLoopGroup());
    serverBootstrap.channel(NioServerSocketChannel.class);
    serverBootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(serverCodec);
        ch.pipeline().addLast(ImmediateEventExecutor.INSTANCE, new AutoFlusher());
        ch.pipeline().addLast(new ServerHandler(serverExecutor));
      }
    });
    final Channel server = serverBootstrap.bind(ANY_PORT).awaitUninterruptibly().channel();

    // Client
    final Executor clientExecutor = new ForkJoinPool(
        1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, UNCAUGHT_EXCEPTION_HANDLER, true);
    final SocketAddress address = server.localAddress();
    final Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap.group(new NioEventLoopGroup());
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(clientCodec);
        ch.pipeline().addLast(ImmediateEventExecutor.INSTANCE, new AutoFlusher());
        ch.pipeline().addLast(new ClientHandler(meter, clientExecutor));
      }
    });
    final Channel client = clientBootstrap.connect(address).awaitUninterruptibly().channel();

    // Run until client is closed
    client.closeFuture().await();
  }

  private static class AutoFlusher extends ChannelOutboundHandlerAdapter implements Runnable {

    private final AtomicIntegerFieldUpdater<AutoFlusher> FLUSHED =
        AtomicIntegerFieldUpdater.newUpdater(AutoFlusher.class, "flushed");
    @SuppressWarnings("UnusedDeclaration") private volatile int flushed;

    private ChannelHandlerContext ctx;

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
      super.handlerAdded(ctx);
      this.ctx = ctx;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
                      final ChannelPromise promise)
        throws Exception {
      super.write(ctx, msg, promise);
      if (flushed == 0) {
        if (FLUSHED.compareAndSet(this, 0, 1)) {
          ctx.channel().eventLoop().execute(this);
        }
      }
    }

    @Override
    public void run() {
      flushed = 0;
      ctx.flush();
    }
  }

  private static class ServerHandler extends ChannelInboundHandlerAdapter {

    private final Executor executor;

    public ServerHandler(final Executor executor) {
      this.executor = executor;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final ZMTPIncomingMessage message = (ZMTPIncomingMessage) msg;
      executor.execute(new Runnable() {
        @Override
        public void run() {
          ctx.write(message.message());
        }
      });
    }
  }

  private static class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final int CONCURRENCY = 1000;

    private static final ZMTPMessage REQUEST_TEMPLATE = ZMTPMessage.from(true, asList(
        ZMTPFrame.from("envelope1"), ZMTPFrame.from("envelope2"),
        ZMTPFrame.from(""),
        ZMTPFrame.from(EMPTY_BUFFER), // timestamp placeholder
        ZMTPFrame.from(Strings.repeat("d", 20)),
        ZMTPFrame.from(Strings.repeat("d", 40)),
        ZMTPFrame.from(Strings.repeat("d", 100))));

    private final ProgressMeter meter;
    private final Executor executor;

    public ClientHandler(final ProgressMeter meter, final Executor executor) {
      this.meter = meter;
      this.executor = executor;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      for (int i = 0; i < CONCURRENCY; i++) {
        ctx.write(req());
      }
    }

    private ZMTPMessage req() {
      REQUEST_TEMPLATE.retain();
      final ByteBuf timestamp = PooledByteBufAllocator.DEFAULT.buffer(8);
      timestamp.writeLong(System.nanoTime());
      REQUEST_TEMPLATE.content().set(0, ZMTPFrame.from(timestamp));
      return REQUEST_TEMPLATE;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final ZMTPIncomingMessage message = (ZMTPIncomingMessage) msg;
      final long timestamp = message.message().content(0).content().readLong();
      final long latency = System.nanoTime() - timestamp;
      meter.inc(1, latency);
      message.release();
      executor.execute(new Runnable() {
        @Override
        public void run() {
          ctx.write(req());
        }
      });
    }
  }
}
