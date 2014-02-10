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

import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.LittleEndianHeapChannelBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;

public class ZMTPUtilsTests {

  @Test
  public void frameSizeTest() {
    for (boolean more : asList(TRUE, FALSE)) {
      for (int size = 0; size < 1024; size++) {
        final ZMTPFrame frame = ZMTPFrame.create(new byte[size]);
        int estimatedSize = ZMTPUtils.frameSize(frame, 1);
        final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        ZMTPUtils.writeFrame(frame, buffer, more, 1);
        int writtenSize = buffer.readableBytes();
        assertEquals(writtenSize, estimatedSize);
      }
    }
  }

  @Test
  public void messageSizeTest() {
    final List<ZMTPFrame> EMPTY = new ArrayList<ZMTPFrame>();
    final List<ZMTPFrame> manyFrameSizes = new ArrayList<ZMTPFrame>();
    for (int i = 0; i < 1024; i++) {
      manyFrameSizes.add(ZMTPFrame.create(new byte[i]));
    }
    @SuppressWarnings("unchecked") final List<List<ZMTPFrame>> frameSets = asList(
        EMPTY,
        asList(ZMTPFrame.create("foo")),
        asList(ZMTPFrame.create("foo"), ZMTPFrame.create("bar")),
        manyFrameSizes);

    for (boolean enveloped : asList(TRUE, FALSE)) {
      for (List<ZMTPFrame> envelope : frameSets) {
        for (List<ZMTPFrame> payload : frameSets) {
          if (payload.isEmpty()) {
            continue;
          }

          final ZMTPMessage message = new ZMTPMessage(envelope, payload);
          int estimatedSize = ZMTPUtils.messageSize(message, enveloped, 1);
          final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
          ZMTPUtils.writeMessage(message, buffer, enveloped, 1);
          int writtenSize = buffer.readableBytes();
          assertEquals(writtenSize, estimatedSize);
        }
      }
    }

  }

  @Test
  public void testWriteLongBE() {
    ChannelBuffer cb = new BigEndianHeapChannelBuffer(8);
    ZMTPUtils.writeLong(cb, 1);
    cmp(cb, 0,0,0,0,0,0,0,1);
  }

  @Test
  public void testWriteLongLE() {
    ChannelBuffer cb = new LittleEndianHeapChannelBuffer(8);
    ZMTPUtils.writeLong(cb, 1);
    cmp(cb, 0,0,0,0,0,0,0,1);
  }

}
