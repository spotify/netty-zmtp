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
import io.netty.buffer.Unpooled;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPWireFormats.wireFormat;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ZMTPWriterTest {

  private final List<Object> out = Lists.newArrayList();

  @Test
  public void testOneFrame() throws Exception {
    final ZMTPWriter writer = ZMTPWriter.create(ZMTP10);
    final ByteBuf buf = Unpooled.buffer();
    writer.reset(buf);

    ByteBuf frame = writer.frame(11, false);
    assertThat(frame, is(sameInstance(buf)));

    final ByteBuf content = copiedBuffer("hello world", UTF_8);

    frame.writeBytes(content.duplicate());

    final ZMTPFramingDecoder decoder = new ZMTPFramingDecoder(wireFormat(ZMTP10), new RawDecoder());
    decoder.decode(null, buf, out);

    assertThat(out, hasSize(1));
    assertThat(out, contains((Object) singletonList(content)));
  }

  @Test
  public void testTwoFrames() throws Exception {
    final ZMTPWriter writer = ZMTPWriter.create(ZMTP10);
    final ByteBuf buf = Unpooled.buffer();
    writer.reset(buf);

    final ByteBuf f0 = copiedBuffer("hello ", UTF_8);
    final ByteBuf f1 = copiedBuffer("hello ", UTF_8);

    writer.frame(f0.readableBytes(), true).writeBytes(f0.duplicate());
    writer.frame(f1.readableBytes(), false).writeBytes(f1.duplicate());

    final ZMTPFramingDecoder decoder = new ZMTPFramingDecoder(wireFormat(ZMTP10), new RawDecoder());
    decoder.decode(null, buf, out);

    assertThat(out, hasSize(1));
    assertThat(out, contains((Object) asList(f0, f1)));
  }

  @Test
  public void testReframe() throws Exception {
    final ZMTPFramingDecoder decoder = new ZMTPFramingDecoder(wireFormat(ZMTP10), new RawDecoder());
    final ZMTPWriter writer = ZMTPWriter.create(ZMTP10);
    final ByteBuf buf = Unpooled.buffer();

    writer.reset(buf);

    // Request a frame with margin in anticipation of a larger payload...
    // ... but write a smaller payload
    final ByteBuf content = copiedBuffer("hello world", UTF_8);
    writer.frame(content.readableBytes() * 2, true).writeBytes(content.duplicate());

    // And rewrite the frame accordingly
    writer.reframe(content.readableBytes(), false);

    // Verify that the message can be parsed
    decoder.decode(null, buf, out);
    assertThat(out, hasSize(1));
    assertThat(out, contains((Object) singletonList(content)));

    // Write and verify another message
    final ByteBuf next = copiedBuffer("next", UTF_8);
    writer.frame(next.readableBytes(), false).writeBytes(next.duplicate());

    out.clear();
    decoder.decode(null, buf, out);
    assertThat(out, hasSize(1));
    assertThat(out, contains((Object) singletonList(next)));
  }


  private class RawDecoder implements ZMTPDecoder {

    private long length;

    private List<ByteBuf> frames = Lists.newArrayList();

    public void header(final long length, final boolean more, final List<Object> out) {
      this.length = length;
    }

    @Override
    public void content(final ByteBuf data, final List<Object> out) {
      if (data.readableBytes() < length) {
        return;
      }
      frames.add(data.readBytes((int) length));
    }

    @Override
    public void finish(final List<Object> out) {
      out.add(frames);
      frames = Lists.newArrayList();
    }

    @Override
    public void close() {
      for (final ByteBuf frame : frames) {
        frame.release();
      }
      frames.clear();
    }
  }
}