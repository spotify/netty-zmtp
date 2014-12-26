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

package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import joptsimple.internal.Strings;

public class VerifyingDecoder implements ZMTPMessageDecoder<Void> {

  private ExpectedOutput expected;

  private int discardIndex;
  private int readIndex;
  private boolean finished;

  public VerifyingDecoder(final ExpectedOutput expected) {
    this.expected = expected;
  }

  public VerifyingDecoder() {
  }

  public void expect(ExpectedOutput expected) {
    this.expected = expected;
  }

  @Override
  public void readFrame(final ByteBuf data, final int size, final boolean more) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    if (readIndex >= expected.frames.size()) {
      throw new IllegalStateException(
          "more frames than expected: " +
          "readIndex=" + readIndex + ", " +
          "expected=" + expected +
          ", readFrame(data=" + data +
          ", size=" + size +
          ", more=" + more + ")");
    }
    final ByteBuf expectedFrame = expected.frames.get(readIndex);
    final int mark = data.writerIndex();
    // TODO: set up writer index (slice?) in ZMTPMessageParser?
    data.writerIndex(data.readerIndex() + size);
    try {
      if (!expectedFrame.equals(data)) {
        throw new IllegalStateException(
            "read frame did not match expected frame: " +
            "readIndex=" + readIndex + ", " +
            "expected frame=" + expectedFrame +
            "read frame=" + data);
      }
    } finally {
      data.writerIndex(mark);
    }
    data.skipBytes(size);
    readIndex++;
  }

  @Override
  public void discardFrame(final int size, final boolean more) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    if (discardIndex >= expected.discarded.size()) {
      throw new IllegalStateException(
          "more than expected discarded frames: " +
          "readIndex=" + readIndex + ", " +
          "discardIndex=" + discardIndex + ", " +
          "expected=" + expected +
          ", discardFrame(size=" + size +
          ", more=" + more + ")");
    }
    discardIndex++;
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
          "discardIndex=" + discardIndex + ", " +
          "expected=" + expected);
    }
    if (discardIndex != expected.discarded.size()) {
      throw new IllegalStateException(
          "less than expected frames discarded: " +
          "readIndex=" + readIndex + ", " +
          "discardIndex=" + discardIndex + ", " +
          "expected=" + expected);
    }
    readIndex = 0;
    discardIndex = 0;
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
    private final List<Integer> discarded;

    public ExpectedOutput(final List<ByteBuf> frames, final List<Integer> discarded) {
      this.frames = frames;
      this.discarded = discarded;
    }

    public ExpectedOutput(final List<ByteBuf> frames) {
      this(frames, Collections.<Integer>emptyList());
    }

    @Override
    public String toString() {
      return "{" +
             "frames=" + toString(frames) +
             ", discarded=" + discarded +
             '}';
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
