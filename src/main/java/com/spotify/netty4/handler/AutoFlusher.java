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

package com.spotify.netty4.handler;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * An outbound Netty 4 channel handler that automatically flushes channel writes. The idea is that
 * by scheduling a flush on the channel event loop, multiple writes can queue up before the actual
 * flush, allowing for a gathering write to an underlying {@link java.nio.channels.GatheringByteChannel},
 * collapsing multiple writes into fewer syscalls.
 */
public class AutoFlusher extends ChannelOutboundHandlerAdapter implements Runnable {

  private final AtomicIntegerFieldUpdater<AutoFlusher> FLUSHED =
      AtomicIntegerFieldUpdater.newUpdater(AutoFlusher.class, "flushed");
  @SuppressWarnings("UnusedDeclaration") private volatile int flushed;

  private ChannelHandlerContext ctx;

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);
    this.ctx = ctx;
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg,
                    final ChannelPromise promise)
      throws Exception {
    super.write(ctx, msg, promise);
    if (flushed == 0) {
      if (FLUSHED.compareAndSet(this, 0, 1)) {
        ctx.channel().eventLoop().execute(this);
      }
    }
  }

  @Override
  public void run() {
    flushed = 0;
    ctx.flush();
  }
}
