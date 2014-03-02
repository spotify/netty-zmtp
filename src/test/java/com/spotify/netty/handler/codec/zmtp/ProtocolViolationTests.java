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

import com.google.common.base.Charsets;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.spotify.netty.handler.codec.zmtp.ZMTPConnectionType.Addressed;

import static org.mockito.Mockito.*;

public class ProtocolViolationTests {

  private EmbeddedChannel serverChannel;
  private String identity = "identity";
  private ChannelInboundHandler mockHandler = mock(ChannelInboundHandler.class);

  @Before
  public void setup() {
    ZMTPSession session = new ZMTPSession(Addressed, identity.getBytes());
    serverChannel = new EmbeddedChannel(
        new ZMTPFramingDecoder(session),
        new ZMTPFramingEncoder(session),
        mockHandler);
  }

  @After
  public void teardown() {
    if (serverChannel != null) {
      serverChannel.close();
    }
  }

  @Test
  public void testBadConnection() throws Exception {
    for (int i = 0; i < 32; i++) {
      testConnect(i);
    }
  }

  private void testConnect(final int payloadSize) throws InterruptedException {
    final StringBuilder payload = new StringBuilder();
    for (int i = 0; i < payloadSize; i++) {
      payload.append('0');
    }

    serverChannel.writeInbound(Unpooled.copiedBuffer(payload, Charsets.UTF_8));

	  // TODO- verify it's ok to remove this
//    verify(mockHandler, never())
//        .channelActive(any(ChannelHandlerContext.class));
    Assert.assertNull(serverChannel.readInbound());
  }
}
