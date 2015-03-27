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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPConnectionType.ADDRESSED;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.REQ;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZMQIntegrationTest {

  private Channel serverChannel;
  private InetSocketAddress serverAddress;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup group;

  private String identity = "identity";

  private BlockingQueue<ZMTPIncomingMessage> incomingMessages =
      new LinkedBlockingQueue<ZMTPIncomingMessage>();

  private BlockingQueue<Channel> channelsConnected =
      new LinkedBlockingQueue<Channel>();

  @Before
  public void setup() {
    final ServerBootstrap b = new ServerBootstrap();
    bossGroup = new NioEventLoopGroup(1);
    group = new NioEventLoopGroup();

    b.channel(NioServerSocketChannel.class);
    b.group(bossGroup, group);
    b.childHandler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(
            ZMTPCodec.builder()
                .protocol(ZMTP20)
                .socketType(REQ)
                .connectionType(ADDRESSED)
                .localIdentity(identity)
                .build(),
            new ChannelInboundHandlerAdapter() {

              @Override
              public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
                  throws Exception {
                if (evt instanceof ZMTPSession) {
                  channelsConnected.add(ctx.channel());
                }
              }

              @Override
              public void channelRead(final ChannelHandlerContext ctx, final Object msg)
                  throws Exception {
                ReferenceCountUtil.releaseLater(msg);
                incomingMessages.put((ZMTPIncomingMessage) msg);
              }
            });
      }
    });

    final InetSocketAddress address = new InetSocketAddress("localhost", 0);
    serverChannel = b.bind(address).awaitUninterruptibly().channel();
    serverAddress = (InetSocketAddress) serverChannel.localAddress();
  }

  @After
  public void teardown() {
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

  @Test
  public void testZmqDealer() throws Exception {
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.DEALER);
    socket.connect("tcp://" + serverAddress.getHostName() + ":" + serverAddress.getPort());
    final ZMsg request = ZMsg.newStringMsg("envelope", "", "hello", "world");
    request.send(socket, false);

    final Channel channel = channelsConnected.take();
    final ZMTPIncomingMessage receivedRequest = incomingMessages.take();
    final ZMTPMessage received = receivedRequest.message();
    received.retain();
    channel.writeAndFlush(received);

    final ZMsg reply = ZMsg.recvMsg(socket);
    final Iterator<ZFrame> reqIter = request.iterator();
    final Iterator<ZFrame> replyIter = reply.iterator();
    while (reqIter.hasNext()) {
      assertTrue(replyIter.hasNext());
      assertArrayEquals(reqIter.next().getData(), replyIter.next().getData());
    }
    assertFalse(replyIter.hasNext());

    assertEquals(1, received.envelope().size());
    assertEquals(2, received.content().size());
    assertEquals(Unpooled.copiedBuffer("envelope", UTF_8), received.envelope().get(0).content());
    assertEquals(Unpooled.copiedBuffer("hello", UTF_8), received.content().get(0).content());
    assertEquals(Unpooled.copiedBuffer("world", UTF_8), received.content().get(1).content());
  }

  @Test
  public void testZmqRouter() throws Exception {
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
    socket.connect("tcp://" + serverAddress.getHostName() + ":" + serverAddress.getPort());

    final ZMTPMessage request = new ZMTPMessage(
        asList(ZMTPFrame.from("envelope")),
        asList(ZMTPFrame.from("hello"), ZMTPFrame.from("world")));

    final Channel channel = channelsConnected.take();
    channel.writeAndFlush(request);

    final ZMsg receivedReply = ZMsg.recvMsg(socket);

    assertEquals(ZMsg.newStringMsg(identity, "envelope", "", "hello", "world"), receivedReply);
  }


}
