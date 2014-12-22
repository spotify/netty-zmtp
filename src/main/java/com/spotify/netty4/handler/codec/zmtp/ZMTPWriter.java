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
  private final boolean enveloped;
  private final Output output;

  private int expectedEnvelopes;
  private int expectedContents;

  private int envelope;
  private int content;

  private int size;
  private ByteBuf buf;

  public ZMTPWriter(final ZMTPSession session, final ByteBufAllocator allocator) {
    this(session, new ByteBufAllocatorOutput(allocator));
  }

  public ZMTPWriter(final ZMTPSession session, final Output output) {
    this.version = session.actualVersion();
    this.enveloped = session.isEnveloped();
    this.output = output;
    if (output == null) {
      throw new NullPointerException("output");
    }
  }

  public void reset() {
    expectedEnvelopes = 0;
    expectedContents = 0;
    envelope = 0;
    content = 0;
    buf = null;
    size = 0;
  }

  public void expectEnvelope(final int size) {
    expectedEnvelopes++;
    this.size += ZMTPUtils.frameSize(size, version);
  }

  public void expectContent(final int size) {
    expectedContents++;
    this.size += ZMTPUtils.frameSize(size, version);
  }

  public void expectEnvelopes(final int envelopes) {
    this.expectedEnvelopes = envelopes;
  }

  public void expectContents(final int contents) {
    this.expectedContents = contents;
  }

  public void expectTotalSize(final int size) {
    this.size = size;
  }

  public void beginEnvelopes() {
    if (enveloped) {
      size += ZMTPUtils.frameSize(0, version);
    }
    buf = output.buffer(size);
  }

  public void endEnvelopes() {
  }

  public ByteBuf envelope(final int size) {
    if (envelope >= expectedEnvelopes) {
      throw new IllegalStateException("content");
    }
    return envelope(size, enveloped);
  }

  private ByteBuf envelope(final int size, final boolean more) {
    ZMTPUtils.writeFrameHeader(buf, size, more, version);
    envelope++;
    return buf;
  }

  public void beginContents() {
    if (enveloped) {
      ZMTPUtils.writeFrameHeader(buf, 0, true, version);
    }
  }

  public ByteBuf content(final int size) {
    if (content >= expectedContents) {
      throw new IllegalStateException("content");
    }
    final boolean more = (content + 1 < expectedContents);
    return content(size, more);
  }

  private ByteBuf content(final int size, final boolean more) {
    ZMTPUtils.writeFrameHeader(buf, size, more, version);
    content++;
    return buf;
  }

  public void endContents() {
  }

  public ByteBuf finish() {
    return buf;
  }
}
