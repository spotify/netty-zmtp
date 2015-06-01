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

class Fragmenter {

  interface Consumer {
    void fragments(int[] limits, int count) throws Exception;
  }

  private final int[] limits;
  private final int length;

  public Fragmenter(final int length) {
    this.limits = new int[length];
    this.length = length;
  }

  public void fragment(final Consumer consumer) throws Exception {
    fragment(consumer, 0, 0);
  }

  private void fragment(final Consumer consumer, final int count, final int limit)
      throws Exception {
    if (limit == length) {
      consumer.fragments(limits, count);
      return;
    }

    for (int o = limit + 1; o <= length; o++) {
      limits[count] = o;
      fragment(consumer, count + 1, o);
    }
  }
}
