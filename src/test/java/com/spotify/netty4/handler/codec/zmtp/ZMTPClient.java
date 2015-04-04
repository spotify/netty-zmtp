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
import com.google.common.util.concurrent.SettableFuture;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;

import static com.google.common.collect.Queues.newLinkedBlockingQueue;
import static io.netty.util.ReferenceCountUtil.retain;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A simple ZMTP client for testing purposes.
 */
public class ZMTPClient implements Closeable, ZMTPSocket {

  private final ZMTPCodec codec;
  private final InetSocketAddress address;

  private ChannelFuture future;
  private final SettableFuture<Channel> channel = SettableFuture.create();
  private final BlockingQueue<ZMTPIncomingMessage> incomingMessages = newLinkedBlockingQueue();
  private final SettableFuture<ZMTPSession> incomingSession = SettableFuture.create();

  public ZMTPClient(final ZMTPCodec codec, final InetSocketAddress address) {
    this.codec = codec;
    this.address = address;
  }

  public void start() {
    final Bootstrap bootstrap = new Bootstrap();
    final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    bootstrap.group(eventLoopGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(codec, new Handler(ch));
      }
    });
    future = bootstrap.connect(address);
  }

  @Override
  public void close() {
    if (future != null) {
      future.channel().close();
    }
  }


  @Override
  public void send(final ZMTPMessage request) throws InterruptedException, TimeoutException {
    try {
      channel.get(30, SECONDS).writeAndFlush(request);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ZMTPIncomingMessage recv() throws InterruptedException {
    return incomingMessages.take();
  }

  @Override
  public ByteBuffer remoteIdentity() throws InterruptedException {
    try {
      return incomingSession.get().remoteIdentity();
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private class Handler extends MessageToMessageDecoder<ZMTPIncomingMessage> {

    private final NioSocketChannel ch;

    public Handler(final NioSocketChannel ch) {this.ch = ch;}

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
        throws Exception {
      if (evt instanceof ZMTPSession) {
        incomingSession.set((ZMTPSession) evt);
        channel.set(ch);
      }
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ZMTPIncomingMessage msg,
                          final List<Object> out)
        throws Exception {
      incomingMessages.put(retain(msg));
    }
  }
}
