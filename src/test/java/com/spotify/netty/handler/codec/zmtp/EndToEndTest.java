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

package com.spotify.netty.handler.codec.zmtp;

import com.google.common.collect.Queues;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import static com.spotify.netty.handler.codec.zmtp.ZMTPSession.DEFAULT_SIZE_LIMIT;
import static com.spotify.netty.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty.handler.codec.zmtp.ZMTPSocketType.ROUTER;
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
    final ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory());
    bootstrap.setPipeline(Channels.pipeline(codec, handler));
    return bootstrap.bind(address);
  }

  private Channel connect(final SocketAddress address, final ChannelHandler codec,
                          final ChannelHandler handler) {
    final ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory());
    bootstrap.setPipeline(Channels.pipeline(codec, handler));
    return bootstrap.connect(address).getChannel();
  }

  public void testRequestReply(final ChannelHandler serverCodec, final ChannelHandler clientCodec)
      throws InterruptedException {

    // Set up server & client
    Handler server = new Handler();
    Handler client = new Handler();
    Channel serverChannel = bind(ANY_PORT, serverCodec, server);
    SocketAddress address = serverChannel.getLocalAddress();
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
    clientChannel.write(helloWorldMessage());
    ZMTPIncomingMessage receivedRequest = server.messages.poll(5, SECONDS);
    assertThat(receivedRequest, is(notNullValue()));
    assertThat(receivedRequest.message(), is(helloWorldMessage()));

    // Send and receive reply
    serverConnectedChannel.write(fooBarMessage());
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
  public void testZMTP10_RouterDealer() throws InterruptedException {
    ZMTP10Codec server = new ZMTP10Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, ROUTER));
    ZMTP10Codec client = new ZMTP10Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, DEALER));
    testRequestReply(server, client);
  }

  @Test
  public void testZMTP20_RouterDealer_WithInterop() throws InterruptedException {
    ZMTP20Codec server = new ZMTP20Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, ROUTER), INTEROP_ON);
    ZMTP20Codec client = new ZMTP20Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, DEALER), INTEROP_ON);
    testRequestReply(server, client);
  }

  @Test
  public void test_ZMTP20Server_ZMTP10Client_RouterDealer() throws InterruptedException {
    ZMTP20Codec server = new ZMTP20Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, ROUTER), INTEROP_ON);
    ZMTP10Codec client = new ZMTP10Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, DEALER));
    testRequestReply(server, client);
  }

  @Test
  public void testZMTP20_RouterDealer_WithNoInterop() throws InterruptedException {
    ZMTP20Codec server = new ZMTP20Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, ROUTER), INTEROP_OFF);
    ZMTP20Codec client = new ZMTP20Codec(new ZMTPSession(
        DEFAULT_SIZE_LIMIT, NO_IDENTITY, DEALER), INTEROP_OFF);
    testRequestReply(server, client);
  }

  private ZMTPMessage helloWorldMessage() {
    return ZMTPMessage.fromStringsUTF8("", "hello", "world");
  }

  private ZMTPMessage fooBarMessage() {
    return ZMTPMessage.fromStringsUTF8("", "foo", "bar");
  }

  private static class Handler extends SimpleChannelUpstreamHandler {

    private BlockingQueue<Channel> connected = Queues.newLinkedBlockingQueue();
    private BlockingQueue<ZMTPIncomingMessage> messages = Queues.newLinkedBlockingQueue();

    @Override
    public void channelConnected(final ChannelHandlerContext ctx,
                                 final ChannelStateEvent e) throws Exception {
      super.channelConnected(ctx, e);
      connected.add(ctx.getChannel());
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
        throws Exception {
      messages.put((ZMTPIncomingMessage) e.getMessage());
    }
  }
}
