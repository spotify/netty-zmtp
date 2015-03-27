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

import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.List;

import io.netty.util.AbstractReferenceCounted;

import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;

public class ZMTPMessage extends AbstractReferenceCounted {

  private final List<ZMTPFrame> frames;

  public ZMTPMessage(final List<ZMTPFrame> frames) {
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
  public static ZMTPMessage fromStrings(final Charset charset, final String... frames) {
    return fromStrings(charset, asList(frames));
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage fromStrings(final Charset charset, final List<String> frames) {
    return from(new AbstractList<ZMTPFrame>() {
      @Override
      public ZMTPFrame get(final int index) {
        return ZMTPFrame.from(frames.get(index), charset);
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

  public int size() {
    return frames.size();
  }

  /**
   * @return Current list of content in the message
   */
  public List<ZMTPFrame> frames() {
    return frames;
  }

  /**
   * Get a specific frame.
   */
  public ZMTPFrame frame(final int i) {
    return frames.get(i);
  }

  @Override
  protected void deallocate() {
    for (final ZMTPFrame frame : frames) {
      frame.release();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPMessage that = (ZMTPMessage) o;

    if (frames != null ? !frames.equals(that.frames) : that.frames != null) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    return frames != null ? frames.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "ZMTPMessage{" + ZMTPUtils.toString(frames) + '}';
  }
}
