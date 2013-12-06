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

import com.google.common.collect.Lists;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import java.util.List;

import static org.jboss.netty.util.CharsetUtil.UTF_8;
import static org.junit.Assert.assertEquals;

public class FragmenterTest {

  static final String INPUT = "abcd";

  static final String[][] OUTPUT = {
      {"a", "b", "c", "d"},
      {"a", "b", "cd"},
      {"a", "bc", "d"},
      {"a", "bcd"},
      {"ab", "c", "d"},
      {"ab", "cd"},
      {"abc", "d"},
      {"abcd"},
  };

  @Test
  public void test() {
    final ChannelBuffer buffer = ChannelBuffers.copiedBuffer(INPUT, UTF_8);
    final Fragmenter fragmenter = new Fragmenter(buffer);
    for (int i = 0; i < OUTPUT.length; i++) {
      final List<ChannelBuffer> expectedFragments = Lists.newArrayList();
      for (final String fragmentString : OUTPUT[i]) {
        final ChannelBuffer fragment = ChannelBuffers.copiedBuffer(fragmentString, UTF_8);
        expectedFragments.add(fragment);
      }

      final List<ChannelBuffer> fragments = fragmenter.next();
      final List<String> fragmentStrings = Lists.newArrayList();
      for (final ChannelBuffer b : fragments) {
        fragmentStrings.add(b.duplicate().toString(UTF_8));
      }
      assertEquals(expectedFragments, fragments);
    }

  }
}
