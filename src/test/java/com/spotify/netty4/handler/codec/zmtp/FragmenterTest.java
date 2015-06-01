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

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FragmenterTest {

  private static final int SIZE = 4;

  private static final int[][] EXPECTED = {
      {1, 2, 3, 4},
      {1, 2, 4},
      {1, 3, 4},
      {1, 4},
      {2, 3, 4},
      {2, 4},
      {3, 4},
      {4},
  };

  @Test
  public void test() throws Exception {
    final List<int[]> output = Lists.newArrayList();
    final Fragmenter fragmenter = new Fragmenter(SIZE);
    fragmenter.fragment(new Fragmenter.Consumer() {
      @Override
      public void fragments(final int[] limits, final int count) {
        output.add(Arrays.copyOf(limits, count));
      }
    });

    assertEquals(EXPECTED.length, output.size());
    for (int i = 0; i < EXPECTED.length; i++) {
      assertArrayEquals(EXPECTED[i], output.get(i));
    }
  }
}
