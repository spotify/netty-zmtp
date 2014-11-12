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
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
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
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        return Channels.pipeline(
            new ExecutionHandler(executor),
            new ZMTP20Codec(new ZMTPSession(1024, identity.getBytes(),
                                            ZMTPSocketType.REQ), false),
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
    request.send(socket, false);

    final ZMTPIncomingMessage receivedRequest = incomingMessages.take();
    final ZMTPMessage receivedMessage = receivedRequest.message();
    receivedRequest.session().channel().write(receivedMessage);

    final ZMsg reply = ZMsg.recvMsg(socket);
    Iterator<ZFrame> reqIter = request.iterator();
    Iterator<ZFrame> replyIter = reply.iterator();
    while (reqIter.hasNext()) {
      assertTrue(replyIter.hasNext());
      assertArrayEquals(reqIter.next().getData(), replyIter.next().getData());
    }
    assertFalse(replyIter.hasNext());

    assertEquals(4, receivedMessage.size());
    assertEquals(utf8("envelope"), receivedMessage.frame(0).data());
    assertEquals(utf8(""), receivedMessage.frame(1).data());
    assertEquals(utf8("hello"), receivedMessage.frame(2).data());
    assertEquals(utf8("world"), receivedMessage.frame(3).data());
  }

  private ChannelBuffer utf8(final String s) {
    return ChannelBuffers.copiedBuffer(s, UTF_8);
  }

  @Test
  public void testZmqRouter() throws Exception {
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
    socket.connect("tcp://" + serverAddress.getHostName() + ":" + serverAddress.getPort());

    final ZMTPMessage request = ZMTPMessage.fromStringsUTF8("envelope", "", "hello", "world");

    final Channel channel = channelsConnected.take();
    channel.write(request);

    final ZMsg receivedReply = ZMsg.recvMsg(socket);

    assertEquals(ZMsg.newStringMsg(identity, "envelope", "", "hello", "world"), receivedReply);
  }


}
