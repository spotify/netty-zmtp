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

/**
 * An incoming ZMTP message.
 */
public class ZMTPIncomingMessage {

  private final ZMTPMessage message;
  private final boolean truncated;
  private final long byteSize;

  public ZMTPIncomingMessage(final ZMTPMessage message,
                             final boolean truncated, final long byteSize) {
    this.message = message;
    this.truncated = truncated;
    this.byteSize = byteSize;
  }

  /**
   * Get the total size in bytes of the message, including truncated frames.
   */
  public long byteSize() {
    return byteSize;
  }

  /**
   * Return the message.
   *
   * @return The message.
   */
  public ZMTPMessage message() {
    return message;
  }

  /**
   * Check if this incoming message was truncated during parsing due to exceeding the size limit.
   *
   * @return True if truncated, false otherwise.
   */
  public boolean isTruncated() {
    return truncated;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPIncomingMessage that = (ZMTPIncomingMessage) o;

    if (byteSize != that.byteSize) { return false; }
    if (truncated != that.truncated) { return false; }
    if (message != null ? !message.equals(that.message) : that.message != null) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    int result = message != null ? message.hashCode() : 0;
    result = 31 * result + (truncated ? 1 : 0);
    result = 31 * result + (int) (byteSize ^ (byteSize >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "ZMTPIncomingMessage{" +
           "message=" + message +
           ", truncated=" + truncated +
           ", byteSize=" + byteSize +
           '}';
  }
}
