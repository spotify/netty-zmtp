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

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

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
                          msg.session().getRemoteIdentity());

        System.err.println(msg);

        // Verify that frames received is correct
        assertEquals(2, msg.message().size());
        assertEquals(ChannelBuffers.wrappedBuffer(f.getData()), msg.message().frame(1).data());

        f.destroy();

        return true;
      }
    };

    assertEquals(true, tester.connectAndReceive("127.0.0.1", 10001, ZMQ.REQ));
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
                          msg.session().getRemoteIdentity());

        // Verify that frames received is correct (+1 envelope delimiter)
        assertEquals(m.size() + 1, msg.message().size());

        for (final ZFrame f : m) {
          assertEquals(ChannelBuffers.wrappedBuffer(f.getData()),
                       msg.message().frame(framePos + 1).data());
          framePos++;
        }

        return true;
      }
    };

    assertEquals(true, tester.connectAndReceive("127.0.0.1", 4711, ZMQ.REQ));
  }
}
