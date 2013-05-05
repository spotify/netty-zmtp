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

package com.spotify.netty.handler.queue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.queue.BufferedWriteHandler;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A channel handler that attempts to batch together and consolidate smaller writes to avoid many
 * small individual writes on the channel and the syscall overhead this would incur.
 */
public class AutoFlushingWriteBatcher extends BufferedWriteHandler {

  private static final long DEFAULT_INTERVAL = 1;
  private static final TimeUnit DEFAULT_INTERVAL_TIMEUNIT = TimeUnit.MILLISECONDS;
  private static final long DEFAULT_MAX_DELAY = 100;
  private static final TimeUnit DEFAULT_MAX_DELAY_TIMEUNIT = TimeUnit.MICROSECONDS;
  private static final boolean DEFAULT_CONSOLIDATE_ON_FLUSH = true;
  private static final int DEFAULT_MAX_BUFFER_SIZE = 4096;

  private final AtomicInteger bufferSize = new AtomicInteger();
  private final long intervalNanos;
  private final long maxDelayNanos = DEFAULT_MAX_DELAY_TIMEUNIT.toNanos(DEFAULT_MAX_DELAY);
  private final int maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;

  private volatile long lastFlush;
  private volatile long lastWrite;

  private static final ScheduledThreadPoolExecutor flusher =
      new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @NotNull
        @Override
        public Thread newThread(final Runnable r) {
          final Thread thread = new Thread(r);
          thread.setDaemon(true);
          thread.setName("netty-write-buffer-flusher");
          return thread;
        }
      });

  /**
   * Scheduled to be called regularly to enforce the max delay of outgoing messages in the buffer.
   */
  private final Runnable flushTask = new Runnable() {
    @Override
    public void run() {
      // Flush if the buffer has not been flushed during the last max delay time interval
      final long nanosSinceLastFlush = System.nanoTime() - lastFlush;
      if (nanosSinceLastFlush > maxDelayNanos) {
        flush();
      }
    }
  };

  /**
   * Create a write batcher with default parameters.
   */
  public AutoFlushingWriteBatcher() {
    this(DEFAULT_CONSOLIDATE_ON_FLUSH);
  }

  /**
   * Create a write batcher with custom flushing interval, i.e. the maximum amount of time between a
   * message being written to the buffer and the buffer being flushed.
   *
   * @param interval     The flush interval.
   * @param intervalUnit The time unit of the flush interval.
   */
  public AutoFlushingWriteBatcher(final long interval, final TimeUnit intervalUnit) {
    this(interval, intervalUnit, DEFAULT_CONSOLIDATE_ON_FLUSH);
  }

  /**
   * Create a write batcher, controlling whether it consolidates buffers when flushing. I.e., if the
   * channel buffers are joined and written to a single channel buffer when flushing. NIO scatter
   * writes are slow enough that consolidating buffers seem to always be the most performant
   * option.
   *
   * @param consolidateOnFlush true if buffers should be consolidated on flush, false otherwise.
   */
  public AutoFlushingWriteBatcher(final boolean consolidateOnFlush) {
    this(DEFAULT_INTERVAL, DEFAULT_INTERVAL_TIMEUNIT, consolidateOnFlush);
  }

  /**
   * Create a write batcher with custom flushing interval, i.e. the maximum amount of time between a
   * message being written to the buffer and the buffer being flushed as well as if the buffers
   * should be consolidated when flushing. I.e., if the channel buffers are joined and written to a
   * single channel buffer when flushing. NIO scatter writes are slow enough that consolidating
   * buffers seem to always be the most performant option.
   *
   * @param interval           The flush interval.
   * @param intervalUnit       The time unit of the flush interval.
   * @param consolidateOnFlush true if buffers should be consolidated on flush, false otherwise.
   */
  public AutoFlushingWriteBatcher(final long interval, final TimeUnit intervalUnit,
                                  final boolean consolidateOnFlush) {
    super(consolidateOnFlush);
    this.intervalNanos = intervalUnit.toNanos(interval);
  }

  /**
   * Called when the channel is opened.
   */
  @Override
  public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e)
      throws Exception {
    super.channelOpen(ctx, e);
    // Schedule a task to flush and enforce the maximum latency that a message is buffered
    flusher.scheduleWithFixedDelay(flushTask, intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Called when the channel is closed.
   */
  @Override
  public void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e)
      throws Exception {
    super.channelClosed(ctx, e);
    // Remove the scheduled flushing task.
    flusher.remove(flushTask);
  }

  /**
   * Called when an outgoing message is written to the channel.
   */
  @Override
  public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e)
      throws Exception {
    super.writeRequested(ctx, e);

    // Calculate new size of outgoing message buffer
    final ChannelBuffer data = (ChannelBuffer) e.getMessage();
    final int newBufferSize = bufferSize.addAndGet(data.readableBytes());

    // Calculate how long it was since the last outgoing message
    final long now = System.nanoTime();
    final long nanosSinceLastWrite = now - lastWrite;
    lastWrite = now;

    // Flush if writes are sparse or if the buffer has reached its threshold size
    if (nanosSinceLastWrite > maxDelayNanos ||
        newBufferSize > maxBufferSize) {
      flush();
    }
  }

  @Override
  public void flush() {
    super.flush();

    // The message buffer is now empty
    bufferSize.set(0);

    // Record the flush time for use in the scheduled flush task
    lastFlush = System.nanoTime();
  }
}