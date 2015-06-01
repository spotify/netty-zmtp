/*
 * Copyright (c) 2012-2015 Spotify AB
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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class VerifyingDecoder implements ZMTPDecoder {

  private ExpectedOutput expected;

  private int readIndex;
  private boolean finished;
  private long frameSize;

  public VerifyingDecoder(final ExpectedOutput expected) {
    this.expected = expected;
  }

  public VerifyingDecoder() {
  }

  public void expect(ExpectedOutput expected) {
    this.expected = expected;
  }

  @Override
  public void header(final ChannelHandlerContext ctx, final long length, final boolean more,
                     final List<Object> out) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    if (readIndex >= expected.message.size()) {
      throw new IllegalStateException(
          "more frames than expected: " +
          "readIndex=" + readIndex + ", " +
          "expected=" + expected +
          ", frame(size=" + length +
          ", more=" + more + ")");
    }
    frameSize = length;
  }

  @Override
  public void content(final ChannelHandlerContext ctx, final ByteBuf data, final List<Object> out) {
    if (data.readableBytes() < frameSize) {
      return;
    }
    final ByteBuf expectedFrame = expected.message.frame(readIndex);
    final ByteBuf frame = data.readBytes((int) frameSize);
    if (!expectedFrame.equals(frame)) {
      throw new IllegalStateException(
          "read frame did not match expected frame: " +
          "readIndex=" + readIndex + ", " +
          "expected frame=" + expectedFrame +
          "read frame=" + frame);
    }
    readIndex++;
  }

  @Override
  public void finish(final ChannelHandlerContext ctx, final List<Object> out) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    if (readIndex != expected.message.size()) {
      throw new IllegalStateException(
          "less than expected frames read: " +
          "readIndex=" + readIndex + ", " +
          "expected=" + expected);
    }
    readIndex = 0;
    finished = true;
  }

  @Override
  public void close() {
  }

  public void assertFinished() {
    if (!finished) {
      throw new AssertionError("not finished");
    }
    finished = false;
  }

  static class ExpectedOutput {

    private final ZMTPMessage message;

    public ExpectedOutput(final ZMTPMessage message) {
      this.message = message;
    }

    @Override
    public String toString() {
      return message.toString();
    }
  }
}
