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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import joptsimple.internal.Strings;

public class VerifyingDecoder2 implements ZMTPMessageDecoder2 {

  private ExpectedOutput expected;

  private int readIndex;
  private boolean finished;
  private int frameSize;

  public VerifyingDecoder2(final ExpectedOutput expected) {
    this.expected = expected;
  }

  public VerifyingDecoder2() {
  }

  public void expect(ExpectedOutput expected) {
    this.expected = expected;
  }

  @Override
  public void frame(final int length, final boolean more) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    if (readIndex >= expected.frames.size()) {
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
  public void content(final ByteBuf data) {
    if (data.readableBytes() < frameSize) {
      return;
    }
    final ByteBuf expectedFrame = expected.frames.get(readIndex);
    final ByteBuf frame = data.readBytes(frameSize);
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
  public Void finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    if (readIndex != expected.frames.size()) {
      throw new IllegalStateException(
          "less than expected frames read: " +
          "readIndex=" + readIndex + ", " +
          "expected=" + expected);
    }
    readIndex = 0;
    finished = true;
    return null;
  }

  public void assertFinished() {
    if (!finished) {
      throw new AssertionError("not finished");
    }
    finished = false;
  }

  static class ExpectedOutput {

    private final List<ByteBuf> frames;

    public ExpectedOutput(final List<ByteBuf> frames) {
      this.frames = frames;
    }

    @Override
    public String toString() {
      return '[' + toString(frames) + ']';
    }

    private String toString(final List<ByteBuf> frames) {
      return Strings.join(Lists.transform(frames, new Function<ByteBuf, String>() {
        @Override
        public String apply(final ByteBuf frame) {
          return frame.toString(CharsetUtil.UTF_8);
        }
      }), ", ");
    }
  }
}
