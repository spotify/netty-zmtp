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


import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;

/**
 * Netty encoder for ZMTP messages.
 */
class ZMTPFramingEncoder extends ChannelOutboundHandlerAdapter {

  private final ZMTPSession session;
  private final ZMTPMessageEncoder encoder;

  private List<Object> messages;
  private List<ChannelPromise> promises;

  public ZMTPFramingEncoder(final ZMTPSession session) {
    this(session, new DefaultZMTPMessageEncoder(session.isEnveloped()));
  }

  public ZMTPFramingEncoder(final ZMTPSession session, final ZMTPMessageEncoder encoder) {
    if (session == null) {
      throw new NullPointerException("session");
    }
    if (encoder == null) {
      throw new NullPointerException("encoder");
    }
    this.session = session;
    this.encoder = encoder;
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
      throws Exception {
    if (messages == null) {
      messages = new ArrayList<Object>();
      promises = new ArrayList<ChannelPromise>();
    }
    messages.add(msg);
    promises.add(promise);
  }

  @Override
  public void flush(final ChannelHandlerContext ctx) throws Exception {
    if (messages == null) {
      return;
    }
    final ZMTPEstimator estimator = new ZMTPEstimator(session.actualVersion());
    for (int i = 0; i < messages.size(); i++) {
      final Object message = messages.get(i);
      encoder.estimate(message, estimator);
    }
    final ByteBuf output = ctx.alloc().buffer(estimator.size());
    final ZMTPWriter writer = new ZMTPWriter(session.actualVersion(), output);
    for (int i = 0; i < messages.size(); i++) {
      final Object message = messages.get(i);
      encoder.encode(message, writer);
      ReferenceCountUtil.release(message);
    }
    final List<ChannelPromise> promises = this.promises;
    this.messages = null;
    this.promises = null;
    final ChannelPromise aggregate = new AggregatePromise(ctx.channel(), ctx.executor(), promises);
    ctx.write(output, aggregate);
    ctx.flush();
  }

  private static class AggregatePromise extends DefaultChannelPromise {

    private final List<ChannelPromise> promises;

    public AggregatePromise(final Channel channel,
                            final EventExecutor executor,
                            final List<ChannelPromise> promises) {
      super(channel, executor);
      this.promises = promises;
    }

    @Override
    public ChannelPromise setSuccess(final Void result) {
      super.setSuccess(result);
      for (int i = 0; i < promises.size(); i++) {
        promises.get(i).setSuccess(result);
      }
      return this;
    }

    @Override
    public boolean trySuccess() {
      final boolean result = super.trySuccess();
      for (int i = 0; i < promises.size(); i++) {
        promises.get(i).trySuccess();
      }
      return result;
    }

    @Override
    public ChannelPromise setFailure(final Throwable cause) {
      super.setFailure(cause);
      for (int i = 0; i < promises.size(); i++) {
        promises.get(i).setFailure(cause);
      }
      return this;
    }
  }
}
