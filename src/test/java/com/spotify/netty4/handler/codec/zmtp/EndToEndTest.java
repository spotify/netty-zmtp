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

import static com.spotify.netty4.handler.codec.zmtp.ZMTPConnectionType.ADDRESSED;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class EndToEndTest {

  public static final byte[] NO_IDENTITY = null;

  public static final boolean INTEROP_ON = true;
  private static final boolean INTEROP_OFF = false;

  public static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);

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

  public void testRequestReply(final ChannelHandler serverCodec, final ChannelHandler clientCodec)
      throws InterruptedException {

    // Set up server & client
    Handler server = new Handler();
    Handler client = new Handler();
    Channel serverChannel = bind(ANY_PORT, serverCodec, server);
    SocketAddress address = serverChannel.localAddress();
    Channel clientChannel = connect(address, clientCodec, client);
    Channel clientConnectedChannel = client.connected.poll(5, SECONDS);
    assertThat(clientConnectedChannel, is(notNullValue()));
    Channel serverConnectedChannel = server.connected.poll(5, SECONDS);
    assertThat(serverConnectedChannel, is(notNullValue()));

    // Make sure there's no left over messages/connections on the wires
    Thread.sleep(1000);
    assertThat("unexpected server message", server.messages.poll(), is(nullValue()));
    assertThat("unexpected client message", client.messages.poll(), is(nullValue()));
    assertThat("unexpected server connection", server.connected.poll(), is(nullValue()));
    assertThat("unexpected client connection", client.connected.poll(), is(nullValue()));

    // Send and receive request
    clientChannel.writeAndFlush(helloWorldMessage());
    ZMTPIncomingMessage receivedRequest = server.messages.poll(5, SECONDS);
    assertThat(receivedRequest, is(notNullValue()));
    assertThat(receivedRequest.message(), is(helloWorldMessage()));

    // Send and receive reply
    serverConnectedChannel.writeAndFlush(fooBarMessage());
    ZMTPIncomingMessage receivedReply = client.messages.poll(5, SECONDS);
    assertThat(receivedReply, is(notNullValue()));
    assertThat(receivedReply.message(), is(fooBarMessage()));

    // Make sure there's no left over messages/connections on the wires
    Thread.sleep(1000);
    assertThat("unexpected server message", server.messages.poll(), is(nullValue()));
    assertThat("unexpected client message", client.messages.poll(), is(nullValue()));
    assertThat("unexpected server connection", server.connected.poll(), is(nullValue()));
    assertThat("unexpected client connection", client.connected.poll(), is(nullValue()));
  }

  @Test
  public void test_ZMTP10_Router_ZMTP10_Dealer() throws InterruptedException {
    ZMTPCodec server = ZMTPCodec.of(ZMTP10.withConnectionType(ADDRESSED));
    ZMTPCodec client = ZMTPCodec.of(ZMTP10.withConnectionType(ADDRESSED));
    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20_Router_ZMTP20_Dealer_WithInterop() throws InterruptedException {
    ZMTPCodec server = ZMTPCodec.of(ZMTP20.withSocketType(ROUTER));
    ZMTPCodec client = ZMTPCodec.of(ZMTP20.withSocketType(DEALER));

    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20_Router_ZMTP10_Addressed() throws InterruptedException {
    ZMTPCodec server = ZMTPCodec.of(ZMTP20.withSocketType(ROUTER));
    ZMTPCodec client = ZMTPCodec.of(ZMTP10.withConnectionType(ADDRESSED));
    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20_Router_ZMTP20_Dealer_WithNoInterop() throws InterruptedException {
    ZMTPCodec server = ZMTPCodec.of(
        ZMTP20.builder()
            .interop(false)
            .socketType(ROUTER)
            .build());

    ZMTPCodec client = ZMTPCodec.of(
        ZMTP20.builder()
            .interop(false)
            .socketType(DEALER)
            .build());

    testRequestReply(server, client);
  }

  private ZMTPMessage helloWorldMessage() {
    return ZMTPMessage.fromUTF8("", "hello", "world");
  }

  private ZMTPMessage fooBarMessage() {
    return ZMTPMessage.fromUTF8("", "foo", "bar");
  }

  private static class Handler extends ChannelInboundHandlerAdapter {

    private BlockingQueue<Channel> connected = Queues.newLinkedBlockingQueue();
    private BlockingQueue<ZMTPIncomingMessage> messages = Queues.newLinkedBlockingQueue();

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      connected.add(ctx.channel());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      ReferenceCountUtil.releaseLater(msg);
      messages.put((ZMTPIncomingMessage) msg);
    }
  }
}
