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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;

/**
 * Netty ZMTP encoder.
 */
class ZMTPFramingEncoder extends ChannelOutboundHandlerAdapter {

  private final ZMTPEncoder encoder;

  private final List<Object> messages = new ArrayList<Object>();
  private final List<ChannelPromise> promises = new ArrayList<ChannelPromise>();
  private ZMTPWriter writer;
  private ZMTPEstimator estimator;

  ZMTPFramingEncoder(final ZMTPSession session, final ZMTPEncoder encoder) {
    if (session == null) {
      throw new NullPointerException("session");
    }
    if (encoder == null) {
      throw new NullPointerException("encoder");
    }
    this.encoder = encoder;
    this.writer = ZMTPWriter.create(session.negotiatedVersion());
    this.estimator = ZMTPEstimator.create(session.negotiatedVersion());
  }

  public ZMTPFramingEncoder(final ZMTPWireFormat wireFormat, final ZMTPEncoder encoder) {
    if (wireFormat == null) {
      throw new NullPointerException("wireFormat");
    }
    if (encoder == null) {
      throw new NullPointerException("encoder");
    }
    this.encoder = encoder;
    this.writer = new ZMTPWriter(wireFormat);
    this.estimator = new ZMTPEstimator(wireFormat);
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext ctx) {
    encoder.close();
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg,
                    final ChannelPromise promise) {
    messages.add(msg);
    promises.add(promise);
  }

  @Override
  public void flush(final ChannelHandlerContext ctx) throws Exception {
    if (messages == null) {
      return;
    }
    estimator.reset();
    for (final Object message : messages) {
      encoder.estimate(message, estimator);
    }
    final ByteBuf output = ctx.alloc().buffer(estimator.size());
    writer.reset(output);
    for (final Object message : messages) {
      encoder.encode(message, writer);
      ReferenceCountUtil.release(message);
    }
    final List<ChannelPromise> sentPromises = new ArrayList<ChannelPromise>(promises);
    messages.clear();
    promises.clear();
    ctx.writeAndFlush(output).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          for (final ChannelPromise promise : sentPromises) {
            promise.setSuccess();
          }
        } else {
          for (final ChannelPromise promise : sentPromises) {
            promise.setFailure(future.cause());
          }
        }
      }
    });
  }
}
