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

import io.netty.channel.ChannelFuture;
import org.junit.Test;
import org.jeromq.ZFrame;
import org.jeromq.ZMQ;
import org.jeromq.ZMsg;

import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ZMTPFrameIntegrationTest {

    @Test
    public void testOneFrameREP() {
        final UUID remoteId = UUID.randomUUID();
        final ZFrame f = new ZFrame("test-frame".getBytes());

        final ZMTPTestConnector tester = new ZMTPTestConnector() {
            @Override
            public void preConnect(final ZMQ.Socket socket) {
                socket.setIdentity(ZMTPUtils.getBytesFromUUID(remoteId));
            }

            @Override
            public void afterConnect(final ZMQ.Socket socket, final ChannelFuture future) {
                f.sendAndKeep(socket);
            }

            @Override
            public boolean onMessage(final ZMTPIncomingMessage msg) {
                // Verify that we can parse the identity correctly
                assertArrayEquals(ZMTPUtils.getBytesFromUUID(remoteId),
                        msg.getSession().getRemoteIdentity());

                System.err.println(msg);

                // Verify that frames received is correct
                assertEquals(1, msg.getMessage().getContent().size());
                assertEquals(0, msg.getMessage().getEnvelope().size());
                assertArrayEquals(f.getData(), msg.getMessage().getContentFrame(0).getData());

                f.destroy();

                return true;
            }
        };

        assertEquals(true, tester.connectAndReceive("127.0.0.1", 10001, ZMQ.REQ));
        System.out.println("WORKING?");
    }

    @Test
    public void testMultipleFrameREP() {
        final UUID remoteId = UUID.randomUUID();

        final ZMTPTestConnector tester = new ZMTPTestConnector() {
            ZMsg m;

            @Override
            public void preConnect(final ZMQ.Socket socket) {
                m = new ZMsg();

                for (int i = 0; i < 16; i++) {
                    m.addString("test-frame-" + i);
                }

                socket.setIdentity(ZMTPUtils.getBytesFromUUID(remoteId));
            }

            @Override
            public void afterConnect(final ZMQ.Socket socket, final ChannelFuture future) {
                m.duplicate().send(socket);
            }

            @Override
            public boolean onMessage(final ZMTPIncomingMessage msg) {
                int framePos = 0;

                // Verify that we can parse the identity correctly
                assertArrayEquals(ZMTPUtils.getBytesFromUUID(remoteId),
                        msg.getSession().getRemoteIdentity());

                // Verify that frames received is correct
                assertEquals(m.size(), msg.getMessage().getContent().size());
                assertEquals(0, msg.getMessage().getEnvelope().size());

                for (final ZFrame f : m) {
                    assertArrayEquals(f.getData(), msg.getMessage().getContentFrame(framePos).getData());
                    framePos++;
                }

                return true;
            }
        };

        assertEquals(true, tester.connectAndReceive("127.0.0.1", 4711, ZMQ.REQ));
    }
}
