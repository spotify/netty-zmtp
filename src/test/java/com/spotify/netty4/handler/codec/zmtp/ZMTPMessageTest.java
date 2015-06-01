/*
 * Copyright (c) 2012-2014 Spotify AB
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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ZMTPMessageTest {

  private final static ByteBufAllocator ALLOC = new UnpooledByteBufAllocator(false);

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> versions() {
    final List<Object[]> versions = new ArrayList<Object[]>();
    for (final ZMTPVersion version : ZMTPVersion.supportedVersions()) {
      versions.add(new Object[]{version});
    }
    return versions;
  }

  @Parameterized.Parameter(0)
  public ZMTPVersion version;

  @Test
  public void testNotEquals() {
    final ZMTPMessage m1 = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    final ZMTPMessage m2 = ZMTPMessage.fromUTF8(ALLOC, "foo", "bar");
    assertThat(m1, is(not(m2)));
  }

  @Test
  public void testEquals() {
    final ZMTPMessage m1 = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    final ZMTPMessage m2 = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    assertThat(m1, is(m2));
  }

  @Test
  public void testIdentityEquals() {
    final ZMTPMessage m = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    assertThat(m, is(m));
  }

  @Test
  public void testWriteAndRead() throws ZMTPParsingException {
    final ZMTPMessage message = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    final ByteBuf buffer = message.write(ALLOC, version);
    final ZMTPMessage read = ZMTPMessage.read(buffer, version);
    assertThat(read, is(message));
  }

  @Test
  public void testWriteAndReadTwoMessages() throws ZMTPParsingException {
    final ZMTPMessage m1 = ZMTPMessage.fromUTF8(ALLOC, "hello", "world");
    final ZMTPMessage m2 = ZMTPMessage.fromUTF8(ALLOC, "foo", "bar");
    final ByteBuf buffer = Unpooled.buffer();
    m1.write(buffer, version);
    m2.write(buffer, version);
    final ZMTPMessage r1 = ZMTPMessage.read(buffer, version);
    final ZMTPMessage r2 = ZMTPMessage.read(buffer, version);
    assertThat(r1, is(m1));
    assertThat(r2, is(m2));
  }

  @Test
  public void testFromStringsUTF8() {
    assertEquals(ZMTPMessage.fromUTF8(ALLOC, ""), message(""));
    assertEquals(ZMTPMessage.fromUTF8(ALLOC, "a"), message("a"));
    assertEquals(ZMTPMessage.fromUTF8(ALLOC, "aa"), message("aa"));
    assertEquals(ZMTPMessage.fromUTF8(ALLOC, "aa", "bb"), message("aa", "bb"));
    assertEquals(ZMTPMessage.fromUTF8(ALLOC, "aa", "", "bb"), message("aa", "", "bb"));
  }

  private ZMTPMessage message(final String... frames) {
    return ZMTPMessage.from(frames(asList(frames)));
  }

  private static List<ByteBuf> frames(final List<String> frames) {
    return Lists.transform(frames, new Function<String, ByteBuf>() {
      @Override
      public ByteBuf apply(final String input) {
        return ByteBufUtil.encodeString(ALLOC, CharBuffer.wrap(input), UTF_8);
      }
    });
  }
}
