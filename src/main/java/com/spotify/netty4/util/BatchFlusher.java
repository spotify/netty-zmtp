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

package com.spotify.netty4.util;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;

/**
 * A helper for doing opportunistic batching of netty channel flushes, allowing for a gathering
 * write to an underlying {@link java.nio.channels.GatheringByteChannel},
 * collapsing multiple writes into fewer syscalls.
 */
public class BatchFlusher {

  private static final int DEFAULT_MAX_PENDING = 64;

  private final Channel channel;
  private final EventLoop eventLoop;
  private final int maxPending;

  private final AtomicIntegerFieldUpdater<BatchFlusher> SCHEDULED =
      AtomicIntegerFieldUpdater.newUpdater(BatchFlusher.class, "scheduled");
  @SuppressWarnings("UnusedDeclaration") private volatile int scheduled;

  private int pending;

  private final Runnable flush = new Runnable() {
    @Override
    public void run() {
      scheduled = 0;
      channel.flush();
    }
  };

  public BatchFlusher(final Channel channel) {
    this(channel, DEFAULT_MAX_PENDING);
  }

  public BatchFlusher(final Channel channel, final int maxPending) {
    this.channel = channel;
    this.maxPending = maxPending;
    this.eventLoop = channel.eventLoop();
  }

  public void flush() {
    if (eventLoop.inEventLoop()) {
      pending++;
      if (pending >= maxPending) {
        pending = 0;
        channel.flush();
      }
    }
    if (scheduled == 0 && SCHEDULED.compareAndSet(this, 0, 1)) {
      eventLoop.execute(flush);
    }
  }
}
