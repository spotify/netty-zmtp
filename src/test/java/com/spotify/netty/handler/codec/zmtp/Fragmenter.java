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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Iterator;
import java.util.List;

class Fragmenter {

  private final List<ChannelBuffer> fragments;
  private final ChannelBuffer input;

  private int length;
  private ChannelBuffer slice;
  private Fragmenter fragmenter;
  private boolean done;

  public Fragmenter(final ChannelBuffer input) {
    this.fragments = Lists.newArrayListWithCapacity(input.readableBytes());
    this.input = input;
    step();
  }

  public List<ChannelBuffer> next() {
    if (done) {
      return null;
    }
    fragments.clear();
    done = next(fragments);
    return ImmutableList.copyOf(fragments);
  }

  private boolean next(final List<ChannelBuffer> fragments) {
    fragments.add(slice.duplicate());

    if (fragmenter == null) {
      return true;
    }

    final boolean done = fragmenter.next(fragments);
    if (done) {
      step();
    }

    return false;
  }

  private void step() {
    length++;
    input.readerIndex(0);
    slice = input.readSlice(length);
    if (!input.readable()) {
      fragmenter = null;
    } else {
      fragmenter = new Fragmenter(input.slice());
    }
  }

  public static Iterable<List<ChannelBuffer>> generator(final ChannelBuffer buffer) {
    return new Iterable<List<ChannelBuffer>>() {
      @Override
      public Iterator<List<ChannelBuffer>> iterator() {
        final Fragmenter fragmenter = new Fragmenter(buffer);
        return new AbstractIterator<List<ChannelBuffer>>() {
          @Override
          protected List<ChannelBuffer> computeNext() {
            List<ChannelBuffer> next = fragmenter.next();
            if (next != null) {
              return next;
            } else {
              return endOfData();
            }
          }
        };
      }
    };
  }
}
