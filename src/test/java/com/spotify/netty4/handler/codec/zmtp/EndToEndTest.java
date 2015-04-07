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

package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.collect.Queues;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class EndToEndTest {

  private static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);

  private Channel bind(final SocketAddress address, final ChannelHandler codec,
                       final ChannelHandler handler) {
    final ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(new NioEventLoopGroup(1), new NioEventLoopGroup());
    bootstrap.channel(NioServerSocketChannel.class);
    bootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(codec, handler);
      }
    });
    return bootstrap.bind(address).awaitUninterruptibly().channel();
  }

  private Channel connect(final SocketAddress address, final ChannelHandler codec,
                          final ChannelHandler handler) {
    final Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(new NioEventLoopGroup());
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(codec, handler);
      }
    });
    return bootstrap.connect(address).awaitUninterruptibly().channel();
  }

  private void testRequestReply(final ChannelHandler serverCodec, final ChannelHandler clientCodec)
      throws InterruptedException {

    // Set up server & client
    final Handler server = new Handler();
    final Handler client = new Handler();
    final SocketAddress address = bind(ANY_PORT, serverCodec, server).localAddress();
    final Channel clientChannel = connect(address, clientCodec, client);
    final Channel serverConnectedChannel = server.connected.poll(5, SECONDS);
    assertThat(serverConnectedChannel, is(notNullValue()));

    // Make sure there's no left over messages/connections on the wires
    Thread.sleep(100);
    assertThat("unexpected server message", server.messages.poll(), is(nullValue()));
    assertThat("unexpected client message", client.messages.poll(), is(nullValue()));
    assertThat("unexpected server connection", server.connected.poll(), is(nullValue()));

    // Send and receive request
    final ZMTPMessage helloWorldMessage = ZMTPMessage.fromUTF8("", "hello", "world");
    clientChannel.writeAndFlush(helloWorldMessage.retain());
    final ZMTPMessage receivedRequest = server.messages.poll(5, SECONDS);
    assertThat(receivedRequest, is(notNullValue()));
    assertThat(receivedRequest, is(helloWorldMessage));
    helloWorldMessage.release();

    // Send and receive reply
    final ZMTPMessage fooBarMessage = ZMTPMessage.fromUTF8("", "foo", "bar");
    serverConnectedChannel.writeAndFlush(fooBarMessage.retain());
    final ZMTPMessage receivedReply = client.messages.poll(5, SECONDS);
    assertThat(receivedReply, is(notNullValue()));
    assertThat(receivedReply, is(fooBarMessage));
    fooBarMessage.release();

    // Make sure there's no left over messages/connections on the wires
    Thread.sleep(100);
    assertThat("unexpected server message", server.messages.poll(), is(nullValue()));
    assertThat("unexpected client message", client.messages.poll(), is(nullValue()));
    assertThat("unexpected server connection", server.connected.poll(), is(nullValue()));
  }

  @Test
  public void test_ZMTP10_Router_VS_ZMTP10_Dealer() throws InterruptedException {
    final ZMTPCodec server = ZMTPCodec.builder()
        .protocol(ZMTP10)
        .socketType(ROUTER)
        .build();

    final ZMTPCodec client = ZMTPCodec.builder()
        .protocol(ZMTP10)
        .socketType(DEALER)
        .build();

    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20_Interop_Router_VS_ZMTP20_Interop_Dealer() throws InterruptedException {
    final ZMTPCodec server = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .interop(true)
        .socketType(ROUTER)
        .build();

    final ZMTPCodec client = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .interop(true)
        .socketType(DEALER)
        .build();

    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20_Interop_Router_VS_ZMTP10_Dealer() throws InterruptedException {
    final ZMTPCodec server = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .interop(true)
        .socketType(ROUTER)
        .build();

    final ZMTPCodec client = ZMTPCodec.builder()
        .protocol(ZMTP10)
        .socketType(DEALER)
        .build();

    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20_NoInterop_Router_VS_ZMTP20_NoInterop_Dealer() throws InterruptedException {
    final ZMTPCodec server = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .interop(false)
        .socketType(ROUTER)
        .build();

    final ZMTPCodec client = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .interop(false)
        .socketType(DEALER)
        .build();

    testRequestReply(server, client);
  }

  private static class Handler extends ChannelInboundHandlerAdapter {

    private final BlockingQueue<Channel> connected = Queues.newLinkedBlockingQueue();
    private final BlockingQueue<ZMTPMessage> messages = Queues.newLinkedBlockingQueue();

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      connected.add(ctx.channel());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      ReferenceCountUtil.releaseLater(msg);
      messages.put((ZMTPMessage) msg);
    }
  }
}
