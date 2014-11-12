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

package com.spotify.netty.handler.codec.zmtp;

import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.ZMTPFrame.EMPTY_FRAME;
import static java.util.Arrays.asList;
import static org.jboss.netty.util.CharsetUtil.UTF_8;

public class ZMTPMessage {

  private final List<ZMTPFrame> frames;

  /**
   * Create a new message from envelope and content frames.
   *
   * @param frames The message frames. Must not be modified again.
   */
  public ZMTPMessage(final List<ZMTPFrame> frames) {
    if (frames == null) {
      throw new IllegalArgumentException("frames");
    }
    this.frames = frames;
  }

  /**
   * Create a new message from a string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromStringsUTF8(final String... frames) {
    return fromStrings(UTF_8, frames);
  }

  /**
   * Create a new message from a list of string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromStringsUTF8(final List<String> frames) {
    return fromStrings(UTF_8, frames);
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage fromStrings(final Charset charset,
                                        final String... frames) {
    return fromStrings(charset, asList(frames));
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage fromStrings(final Charset charset,
                                        final List<String> frames) {
    return from(new AbstractList<ZMTPFrame>() {
      @Override
      public ZMTPFrame get(final int index) {
        return ZMTPFrame.create(frames.get(index), charset);
      }

      @Override
      public int size() {
        return frames.size();
      }
    });
  }

  /**
   * Create a new message from a list of byte array frames.
   */
  public static ZMTPMessage fromByteArrays(final List<byte[]> frames) {
    return from(new AbstractList<ZMTPFrame>() {
      @Override
      public ZMTPFrame get(final int index) {
        byte[] bytes = frames.get(index);
        if (bytes.length == 0) {
          return EMPTY_FRAME;
        }
        return ZMTPFrame.create(bytes);
      }

      @Override
      public int size() {
        return frames.size();
      }
    });
  }

  /**
   * Create a new message from a list of frames.
   */
  public static ZMTPMessage from(final List<ZMTPFrame> frames) {
    return new ZMTPMessage(frames);
  }

  /**
   * Get a list of message frames.
   */
  public List<ZMTPFrame> frames() {
    return Collections.unmodifiableList(frames);
  }

  /**
   * Returns a specific content frame
   *
   * @param index frame to return (0 based)
   * @return ZMTPFrame identified by frameId
   */
  public ZMTPFrame frame(final int index) {
    return frames.get(index);
  }

  /**
   * Return the number of frames in this message.
   */
  public int size() {
    return frames.size();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPMessage that = (ZMTPMessage) o;

    if (!frames.equals(that.frames)) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    return frames.hashCode();
  }

  @Override
  public String toString() {
    return "ZMTPMessage{" + ZMTPUtils.toString(frames) + '}';
  }
}
