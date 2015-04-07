/*
 * Copyright (c) 2012-2015 Spotify AB
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

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import static io.netty.util.CharsetUtil.UTF_8;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ZMTPMessageDecoderTest {

  private final static ByteBufAllocator ALLOC = new UnpooledByteBufAllocator(false);

  @Test
  public void testSingleFrame() throws Exception {
    final ZMTPMessageDecoder decoder = new ZMTPMessageDecoder();

    final ByteBuf content = Unpooled.copiedBuffer("hello", UTF_8);

    final List<Object> out = Lists.newArrayList();
    decoder.header(content.readableBytes(), false, out);
    decoder.content(content, out);
    decoder.finish(out);

    final Object expected = ZMTPMessage.fromUTF8(ALLOC, "hello");
    assertThat(out, hasSize(1));
    assertThat(out, contains(expected));
  }

  @Test
  public void testTwoFrames() throws Exception {
    final ZMTPMessageDecoder decoder = new ZMTPMessageDecoder();

    final ByteBuf f0 = Unpooled.copiedBuffer("hello", UTF_8);
    final ByteBuf f1 = Unpooled.copiedBuffer("world", UTF_8);

    final List<Object> out = Lists.newArrayList();
    decoder.header(f0.readableBytes(), true, out);
    decoder.content(f0, out);
    decoder.header(f1.readableBytes(), false, out);
    decoder.content(f1, out);
    decoder.finish(out);

    final Object expected = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    assertThat(out, hasSize(1));
    assertThat(out, contains(expected));
  }
}