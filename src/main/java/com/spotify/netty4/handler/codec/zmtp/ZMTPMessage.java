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

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.internal.RecyclableArrayList;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;
import static io.netty.buffer.ByteBufUtil.encodeString;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;

public class ZMTPMessage extends AbstractReferenceCounted implements Iterable<ByteBuf> {

  private final ByteBuf[] frames;

  private ZMTPMessage(final ByteBuf[] frames) {
    this.frames = checkNotNull(frames, "frames");
  }

  @Override
  public ZMTPMessage retain() {
    super.retain();
    return this;
  }

  @Override
  public ZMTPMessage retain(final int increment) {
    super.retain(increment);
    return this;
  }

  /**
   * Create a new message from a string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromUTF8(final CharSequence... strings) {
    return from(ByteBufAllocator.DEFAULT, UTF_8, strings);
  }

  /**
   * Create a new message from a string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromUTF8(final ByteBufAllocator alloc, final CharSequence... strings) {
    return from(alloc, UTF_8, strings);
  }

  /**
   * Create a new message from a list of string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromUTF8(final Iterable<? extends CharSequence> strings) {
    return from(UTF_8, strings);
  }

  /**
   * Create a new message from a list of string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromUTF8(final ByteBufAllocator alloc,
                                     final Iterable<? extends CharSequence> strings) {
    return from(alloc, UTF_8, strings);
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage from(final Charset charset, final CharSequence... strings) {
    return from(charset, asList(strings));
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage from(final ByteBufAllocator alloc, final Charset charset,
                                 final CharSequence... strings) {
    return from(alloc, charset, asList(strings));
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage from(final Charset charset,
                                 final Iterable<? extends CharSequence> strings) {
    return from(ByteBufAllocator.DEFAULT, charset, strings);
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage from(final ByteBufAllocator alloc, final Charset charset,
                                 final Iterable<? extends CharSequence> strings) {
    final List<ByteBuf> frames = new ArrayList<ByteBuf>();
    for (final CharSequence string : strings) {
      frames.add(encodeString(alloc, CharBuffer.wrap(string), charset));
    }
    return from(frames);
  }

  /**
   * Create a new message from a list of frames.
   */
  public static ZMTPMessage from(final Collection<ByteBuf> frames) {
    checkNotNull(frames, "frames");
    return new ZMTPMessage(frames.toArray(new ByteBuf[frames.size()]));
  }

  /**
   * Create a new message from a list of frames.
   */
  public static ZMTPMessage from(final ByteBuf[] frames) {
    return new ZMTPMessage(frames.clone());
  }

  public int size() {
    return frames.length;
  }

  @Override
  public Iterator<ByteBuf> iterator() {
    return new FrameIterator();
  }

  /**
   * Get a specific frame.
   */
  public ByteBuf frame(final int i) {
    return frames[i];
  }

  @Override
  protected void deallocate() {
    for (final ByteBuf frame : frames) {
      frame.release();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPMessage byteBufs = (ZMTPMessage) o;

    return Arrays.equals(frames, byteBufs.frames);

  }

  @Override
  public int hashCode() {
    return frames != null ? Arrays.hashCode(frames) : 0;
  }

  @Override
  public String toString() {
    return "ZMTPMessage{" + toString(frames) + '}';
  }

  /**
   * Create a human readable string representation of binary data, keeping printable ascii and hex
   * encoding everything else.
   *
   * @param data The data
   * @return A human readable string representation of the data.
   */
  private static String toString(final ByteBuf data) {
    if (data == null) {
      return null;
    }
    final StringBuilder sb = new StringBuilder();
    for (int i = data.readerIndex(); i < data.writerIndex(); i++) {
      final byte b = data.getByte(i);
      if (b > 31 && b < 127) {
        if (b == '%') {
          sb.append('%');
        }
        sb.append((char) b);
      } else {
        sb.append('%');
        sb.append(String.format("%02X", b));
      }
    }
    return sb.toString();
  }

  /**
   * Create a human readable string representation of a list of ZMTP frames, keeping printable ascii
   * and hex encoding everything else.
   *
   * @param frames The ZMTP frames.
   * @return A human readable string representation of the frames.
   */
  private static String toString(final ByteBuf[] frames) {
    final StringBuilder builder = new StringBuilder("[");
    for (int i = 0; i < frames.length; i++) {
      final ByteBuf frame = frames[i];
      builder.append('"');
      builder.append(toString(frame));
      builder.append('"');
      if (i < frames.length - 1) {
        builder.append(',');
      }
    }
    builder.append(']');
    return builder.toString();
  }

  /**
   * Convenience method for reading a {@link ZMTPMessage} from a {@link ByteBuf}.
   */
  public static ZMTPMessage read(final ByteBuf in, final ZMTPVersion version)
      throws ZMTPParsingException {
    final int mark = in.readerIndex();
    final ZMTPWireFormat wireFormat = ZMTPWireFormats.wireFormat(version);
    final ZMTPWireFormat.Header header = wireFormat.header();
    final RecyclableArrayList frames = RecyclableArrayList.newInstance();
    while (true) {
      final boolean read = header.read(in);
      if (!read) {
        frames.recycle();
        in.readerIndex(mark);
        return null;
      }
      if (in.readableBytes() < header.length()) {
        frames.recycle();
        in.readerIndex(mark);
        return null;
      }
      if (header.length() > Integer.MAX_VALUE) {
        throw new ZMTPParsingException("frame is too large: " + header.length());
      }
      final ByteBuf frame = in.readSlice((int) header.length());
      frame.retain();
      frames.add(frame);
      if (!header.more()) {
        @SuppressWarnings("unchecked") final ZMTPMessage message =
            ZMTPMessage.from((List<ByteBuf>) (Object) frames);
        frames.recycle();
        return message;
      }
    }
  }

  /**
   * Convenience method for writing a {@link ZMTPMessage} to a {@link ByteBuf}.
   */
  public ByteBuf write(final ZMTPVersion version) {
    return write(ByteBufAllocator.DEFAULT, version);
  }

  /**
   * Convenience method for writing a {@link ZMTPMessage} to a {@link ByteBuf}.
   */
  public ByteBuf write(final ByteBufAllocator alloc, final ZMTPVersion version) {
    final ZMTPMessageEncoder encoder = new ZMTPMessageEncoder();
    final ZMTPEstimator estimator = ZMTPEstimator.create(version);
    encoder.estimate(this, estimator);
    final ByteBuf out = alloc.buffer(estimator.size());
    final ZMTPWriter writer = ZMTPWriter.create(version);
    writer.reset(out);
    encoder.encode(this, writer);
    return out;
  }

  /**
   * Convenience method for writing a {@link ZMTPMessage} to a {@link ByteBuf}.
   */
  public void write(final ByteBuf out, final ZMTPVersion version) {
    final ZMTPWriter writer = ZMTPWriter.create(version);
    final ZMTPMessageEncoder encoder = new ZMTPMessageEncoder();
    writer.reset(out);
    encoder.encode(this, writer);
  }

  /**
   * Create a new {@link ZMTPMessage} with a frame added at the front.
   */
  public ZMTPMessage push(final ByteBuf frame) {
    for (final ByteBuf f : frames) {
      f.retain();
    }
    final ByteBuf[] frames = new ByteBuf[this.frames.length + 1];
    frames[0] = frame;
    System.arraycopy(this.frames, 0, frames, 1, this.frames.length);
    return new ZMTPMessage(frames);
  }

  /**
   * Create a new {@link ZMTPMessage} with the front frame removed.
   */
  public ZMTPMessage pop() {
    if (this.frames.length == 0) {
      throw new IllegalStateException("empty message");
    }
    final ByteBuf[] frames = new ByteBuf[this.frames.length - 1];
    System.arraycopy(this.frames, 1, frames, 0, frames.length);
    for (final ByteBuf f : frames) {
      f.retain();
    }
    return new ZMTPMessage(frames);
  }

  /**
   * Iterates over the frames of the {@link ZMTPMessage}.
   */
  private class FrameIterator implements Iterator<ByteBuf> {

    int i;

    @Override
    public boolean hasNext() {
      return i < frames.length;
    }

    @Override
    public ByteBuf next() {
      return frames[i++];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }
}
