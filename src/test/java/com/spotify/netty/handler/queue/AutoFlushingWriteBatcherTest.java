/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.netty.handler.queue;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class AutoFlushingWriteBatcherTest {

  private FlushCountingAutoFlushingWriteBatcher batcher =
      new FlushCountingAutoFlushingWriteBatcher();

  @Mock
  public ChannelHandlerContext ctx;

  @Mock
  public ChannelStateEvent e;

  @Test
  public void shouldStartFlushingOnChannelOpen() throws Exception {
    batcher.channelOpen(ctx, e);

    Thread.sleep(10);

    assertTrue("flush counter incremented", batcher.flushCounter > 0);
  }

  @Test
  public void shouldStopFlushingOnChannelClose() throws Exception {
    batcher.channelOpen(ctx, e);

    Thread.sleep(10);

    batcher.channelClosed(ctx, e);

    verifyFlushingStopped();
  }


  private void verifyFlushingStopped() throws InterruptedException {
    // give the scheduler plenty of time to stop the task
    Thread.sleep(10);

    long flushCount = batcher.flushCounter;

    // wait for another while, and make sure that the counter no longer increases.
    Thread.sleep(10);

    assertThat(batcher.flushCounter, equalTo(flushCount));
  }


  private static class FlushCountingAutoFlushingWriteBatcher extends AutoFlushingWriteBatcher {

    private volatile long flushCounter = 0;

    private FlushCountingAutoFlushingWriteBatcher() {
      super(1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush() {
      flushCounter++;
      super.flush();
    }
  }
}
