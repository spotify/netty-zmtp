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

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ZMQIntegrationTest {

  private ServerBootstrap serverBootstrap;
  private Channel serverChannel;
  private InetSocketAddress serverAddress;

  private String identity = "identity";

  private BlockingQueue<ZMTPIncomingMessage> incomingMessages =
      new LinkedBlockingQueue<ZMTPIncomingMessage>();

  private BlockingQueue<Channel> channelsConnected =
      new LinkedBlockingQueue<Channel>();

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
        final
        ZMTPSession
            session =
            new ZMTPSession(ZMTPConnectionType.Addressed, identity.getBytes());

        return Channels.pipeline(
            new ExecutionHandler(executor),
            new ZMTPFramingDecoder(session),
            new ZMTPFramingEncoder(session),
            new SimpleChannelUpstreamHandler() {

              @Override
              public void channelConnected(final ChannelHandlerContext ctx,
                                           final ChannelStateEvent e) throws Exception {
                super.channelConnected(ctx, e);
                channelsConnected.add(ctx.getChannel());
              }

              @Override
              public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e)
                  throws Exception {
                incomingMessages.put((ZMTPIncomingMessage) e.getMessage());
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
  public void testZmqDealer() throws Exception {
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.DEALER);
    socket.connect("tcp://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
    final ZMsg request = ZMsg.newStringMsg("envelope", "", "hello", "world");
    request.send(socket);

    final ZMTPIncomingMessage receivedRequest = incomingMessages.take();
    final ZMTPMessage receivedMessage = receivedRequest.getMessage();
    receivedRequest.getSession().getChannel().write(receivedMessage);

    final ZMsg reply = ZMsg.recvMsg(socket);
    assertEquals(request, reply);

    assertEquals(1, receivedMessage.getEnvelope().size());
    assertEquals(2, receivedMessage.getContent().size());
    assertArrayEquals("envelope".getBytes(), receivedMessage.getEnvelope().get(0).getData());
    assertArrayEquals("hello".getBytes(), receivedMessage.getContent().get(0).getData());
    assertArrayEquals("world".getBytes(), receivedMessage.getContent().get(1).getData());
  }

  @Test
  public void testZmqRouter() throws Exception {
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
    socket.connect("tcp://" + serverAddress.getHostName() + ":" + serverAddress.getPort());

    final ZMTPMessage request = new ZMTPMessage(
        asList(ZMTPFrame.create("envelope")),
        asList(ZMTPFrame.create("hello"), ZMTPFrame.create("world")));

    final Channel channel = channelsConnected.take();
    channel.write(request);

    final ZMsg receivedReply = ZMsg.recvMsg(socket);

    assertEquals(ZMsg.newStringMsg(identity, "envelope", "", "hello", "world"), receivedReply);
  }


}
