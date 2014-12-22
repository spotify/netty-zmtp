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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ZMTPWriter {

  public interface Output {

    ByteBuf buffer(int size);
  }

  public static class ByteBufAllocatorOutput implements Output {

    private final ByteBufAllocator allocator;

    public ByteBufAllocatorOutput(final ByteBufAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public ByteBuf buffer(final int size) {
      return allocator.buffer(size);
    }
  }

  private final int version;
  private final Output output;

  private int expectedFrames;
  private int expectedSize;
  private ByteBuf buf;

  private int frame;

  public ZMTPWriter(final int version, final ByteBufAllocator allocator) {
    this(version, new ByteBufAllocatorOutput(allocator));
  }

  public ZMTPWriter(final int version, final Output output) {
    this.version = version;
    this.output = output;
    if (output == null) {
      throw new NullPointerException("output");
    }
  }

  public void reset() {
    expectedFrames = 0;
    frame = 0;
    buf = null;
    expectedSize = 0;
  }

  public void expectFrame(final int size) {
    expectedFrames++;
    expectedSize += ZMTPUtils.frameSize(size, version);
  }

  public void expectFrames(final int frames) {
    this.expectedFrames = frames;
  }

  public void expectSize(final int size) {
    this.expectedSize = size;
  }

  public void begin() {
    buf = output.buffer(expectedSize);
  }

  public ByteBuf frame(final int size) {
    if (frame >= expectedFrames) {
      throw new IllegalStateException("content");
    }
    final boolean more = (frame + 1 < expectedFrames);
    return frame(size, more);
  }

  private ByteBuf frame(final int size, final boolean more) {
    ZMTPUtils.writeFrameHeader(buf, size, more, version);
    frame++;
    return buf;
  }

  public void end() {
  }

  public ByteBuf finish() {
    return buf;
  }
}
