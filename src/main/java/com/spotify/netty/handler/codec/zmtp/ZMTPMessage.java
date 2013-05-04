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

import java.util.ArrayList;
import java.util.List;

public class ZMTPMessage {

  List<ZMTPFrame> envelope = new ArrayList<ZMTPFrame>();
  List<ZMTPFrame> content = new ArrayList<ZMTPFrame>();

  /**
   * Creates a new ZMTPMessage from envelope and content frames.
   *
   * @param envelope The envelope frames. Must not be modified again.
   * @param content  The content frames. Must not be modified again.
   */
  public ZMTPMessage(final List<ZMTPFrame> envelope, final List<ZMTPFrame> content) {
    this.envelope = envelope;
    this.content = content;
  }

  /**
   * Return the envelope
   */
  public List<ZMTPFrame> getEnvelope() {
    return envelope;
  }

  /**
   * @return Current list of content in the message
   */
  public List<ZMTPFrame> getContent() {
    return content;
  }

  /**
   * Returns a specific content frame
   *
   * @param frameId frame to return (0 based)
   * @return ZMTPFrame identified by frameId
   */
  public ZMTPFrame getContentFrame(final int frameId) {
    if (frameId < 0 || frameId >= content.size()) {
      throw new IllegalArgumentException("Invalid frame id " + frameId);
    }

    return content.get(frameId);
  }

  /**
   * Helper to convert the object into a string
   */
  @Override
  public String toString() {
    return "ZMTPMessage{" +
           "content=" + content +
           ", envelope=" + envelope +
           '}';
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
