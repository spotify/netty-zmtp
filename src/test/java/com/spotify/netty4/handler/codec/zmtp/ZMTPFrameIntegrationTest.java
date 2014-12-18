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

import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.UUID;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;

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
        socket.setIdentity(ZMTPUtils.encodeUUID(remoteId));
      }

      @Override
      public void afterConnect(final ZMQ.Socket socket, final ChannelFuture future) {
        f.sendAndKeep(socket);
      }

      @Override
      protected void onConnect(final ZMTPSession session) {
        // Verify that we can parse the identity correctly
        assertArrayEquals(ZMTPUtils.encodeUUID(remoteId), session.remoteIdentity());
      }

      @Override
      public boolean onMessage(final ZMTPIncomingMessage msg) {
        System.err.println(msg);

        // Verify that frames received is correct
        assertEquals(1, msg.message().content().size());
        assertEquals(0, msg.message().envelope().size());
        assertEquals(Unpooled.wrappedBuffer(f.getData()), msg.message().contentFrame(0).data());

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

        socket.setIdentity(ZMTPUtils.encodeUUID(remoteId));
      }

      @Override
      public void afterConnect(final ZMQ.Socket socket, final ChannelFuture future) {
        m.duplicate().send(socket);
      }


      @Override
      protected void onConnect(final ZMTPSession session) {
        // Verify that we can parse the identity correctly
        assertArrayEquals(ZMTPUtils.encodeUUID(remoteId), session.remoteIdentity());
      }

      @Override
      public boolean onMessage(final ZMTPIncomingMessage msg) {
        int framePos = 0;

        // Verify that frames received is correct
        assertEquals(m.size(), msg.message().content().size());
        assertEquals(0, msg.message().envelope().size());

        for (final ZFrame f : m) {
          assertEquals(Unpooled.wrappedBuffer(f.getData()),
                       msg.message().contentFrame(framePos).data());
          framePos++;
        }

        return true;
      }
    };

    assertEquals(true, tester.connectAndReceive("127.0.0.1", 4711, ZMQ.REQ));
  }
}
