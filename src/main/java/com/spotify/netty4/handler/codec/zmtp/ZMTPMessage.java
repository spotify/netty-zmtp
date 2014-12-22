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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.netty.util.AbstractReferenceCounted;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPFrame.EMPTY_FRAME;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;

public class ZMTPMessage extends AbstractReferenceCounted {

  private List<ZMTPFrame> envelope;
  private List<ZMTPFrame> content;

  /**
   * Create a new message from envelope and content frames.
   *
   * @param envelope The envelope frames. Must not be modified again.
   * @param content  The content frames. Must not be modified again.
   */
  public ZMTPMessage(final List<ZMTPFrame> envelope, final List<ZMTPFrame> content) {
    this.envelope = envelope;
    this.content = content;
  }

  /**
   * Create a new message from a string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromStringsUTF8(final boolean enveloped, final String... frames) {
    return fromStrings(enveloped, UTF_8, frames);
  }

  /**
   * Create a new message from a list of string frames, using UTF-8 encoding.
   */
  public static ZMTPMessage fromStringsUTF8(final boolean enveloped, final List<String> frames) {
    return fromStrings(enveloped, UTF_8, frames);
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage fromStrings(final boolean enveloped, final Charset charset,
                                        final String... frames) {
    return fromStrings(enveloped, charset, asList(frames));
  }

  /**
   * Create a new message from a list of string frames, using a specified encoding.
   */
  public static ZMTPMessage fromStrings(final boolean enveloped, final Charset charset,
                                        final List<String> frames) {
    return from(enveloped, new AbstractList<ZMTPFrame>() {
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
  public static ZMTPMessage from(final boolean enveloped, final List<ZMTPFrame> frames) {
    final List<ZMTPFrame> head;
    final List<ZMTPFrame> tail = new ArrayList<ZMTPFrame>();
    boolean delimited = false;
    int i = 0;
    if (enveloped) {
      head = new ArrayList<ZMTPFrame>();
      for (; i < frames.size(); i++) {
        final ZMTPFrame frame = frames.get(i);
        if (frame == EMPTY_FRAME) {
          delimited = true;
          i++;
          break;
        }
        head.add(frame);
      }
    } else {
      head = Collections.emptyList();
    }

    for (; i < frames.size(); i++) {
      tail.add(frames.get(i));
    }

    final List<ZMTPFrame> envelope;
    final List<ZMTPFrame> content;
    if (enveloped && !delimited) {
      envelope = Collections.emptyList();
      content = head;
    } else {
      envelope = head;
      content = tail;
    }
    return new ZMTPMessage(envelope, content);
  }

  /**
   * Return the envelope
   */
  public List<ZMTPFrame> envelope() {
    return envelope;
  }

  /**
   * @return Current list of content in the message
   */
  public List<ZMTPFrame> content() {
    return content;
  }

  /**
   * Get a specific envelope frame.
   */
  public ZMTPFrame envelope(final int i) {
    return envelope.get(i);
  }

  /**
   * Get a specific content frame.
   */
  public ZMTPFrame content(final int i) {
    return content.get(i);
  }

  @Override
  protected void deallocate() {
    for (final ZMTPFrame frame : envelope) {
      frame.release();
    }
    for (final ZMTPFrame frame : content) {
      frame.release();
    }
  }

  @Override
  public String toString() {
    return "ZMTPMessage{" + ZMTPUtils.toString(envelope) + "," + ZMTPUtils.toString(content) + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ZMTPMessage that = (ZMTPMessage) o;

    if (content != null ? !content.equals(that.content) : that.content != null) {
      return false;
    }
    if (envelope != null ? !envelope.equals(that.envelope) : that.envelope != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = content != null ? content.hashCode() : 0;
    result = 31 * result + (envelope != null ? envelope.hashCode() : 0);
    return result;
  }
}
