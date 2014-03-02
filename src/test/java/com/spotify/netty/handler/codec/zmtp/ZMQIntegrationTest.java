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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.jeromq.ZFrame;
import org.jeromq.ZMQ;
import org.jeromq.ZMsg;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ZMQIntegrationTest {

    private ServerBootstrap serverBootstrap;
    private Channel serverChannel;
    private InetSocketAddress serverAddress;

    private String identity = "identity";
    private EventLoopGroup bossGroup, workerGroup;

    private BlockingQueue<ZMTPIncomingMessage> incomingMessages =
            new LinkedBlockingQueue<ZMTPIncomingMessage>();

    private BlockingQueue<Channel> channelsConnected =
            new LinkedBlockingQueue<Channel>();

    @Before
    public void setup() throws InterruptedException {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        serverBootstrap = new ServerBootstrap();

        serverBootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ZMTP20Codec(new ZMTPSession(ZMTPConnectionType.Addressed,
                                1024, identity.getBytes(), ZMTPSocketType.REQ), false));
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(final ChannelHandlerContext ctx) throws Exception {
                                super.channelActive(ctx);
                                channelsConnected.add(ctx.channel());
                            }

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                incomingMessages.put((ZMTPIncomingMessage)msg);
                            }
                        });
                    }
                });

        serverChannel = serverBootstrap.bind(0).sync().channel();
        serverAddress = (InetSocketAddress) serverChannel.localAddress();
    }

    @After
    public void teardown() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel.closeFuture().awaitUninterruptibly();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Test
    public void testZmqDealer() throws Exception {
        final ZMQ.Context context = ZMQ.context(1);
        final ZMQ.Socket socket = context.socket(ZMQ.DEALER);
        socket.connect("tcp://0.0.0.0:" + serverAddress.getPort());
        final ZMsg request = ZMsg.newStringMsg("envelope", "", "hello", "world");
        request.send(socket, false);

        final ZMTPIncomingMessage receivedRequest = incomingMessages.take();
        final ZMTPMessage receivedMessage = receivedRequest.getMessage();
        receivedRequest.getSession().getChannel().writeAndFlush(receivedMessage);

        final ZMsg reply = ZMsg.recvMsg(socket);
        Iterator<ZFrame> reqIter = request.iterator();
        Iterator<ZFrame> replyIter = reply.iterator();
        while (reqIter.hasNext()) {
            assertTrue(replyIter.hasNext());
            assertArrayEquals(reqIter.next().data(), replyIter.next().data());
        }
        assertFalse(replyIter.hasNext());

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
        socket.connect("tcp://0.0.0.0:" + serverAddress.getPort());

        final ZMTPMessage request = new ZMTPMessage(
                asList(ZMTPFrame.create("envelope")),
                asList(ZMTPFrame.create("hello"), ZMTPFrame.create("world")));

        final Channel channel = channelsConnected.take();
        channel.writeAndFlush(request);

        final ZMsg receivedReply = ZMsg.recvMsg(socket);

        assertEquals(ZMsg.newStringMsg(identity, "envelope", "", "hello", "world"), receivedReply);
    }


}