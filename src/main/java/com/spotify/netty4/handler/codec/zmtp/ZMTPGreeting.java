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

import java.nio.ByteBuffer;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;

class ZMTPGreeting {

  private final int revision;
  private final ZMTPSocketType socketType;
  private final ByteBuffer identity;

  ZMTPGreeting(final int revision, final ZMTPSocketType socketType, final ByteBuffer identity) {
    this.revision = revision;
    this.socketType = checkNotNull(socketType, "socketType");
    this.identity = checkNotNull(identity, "identity");
  }

  int revision() {
    return revision;
  }

  ZMTPSocketType socketType() {
    return socketType;
  }

  ByteBuffer identity() {
    return identity.asReadOnlyBuffer();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPGreeting that = (ZMTPGreeting) o;

    if (revision != that.revision) { return false; }
    if (socketType != that.socketType) { return false; }
    return !(identity != null ? !identity.equals(that.identity) : that.identity != null);

  }

  @Override
  public int hashCode() {
    int result = revision;
    result = 31 * result + (socketType != null ? socketType.hashCode() : 0);
    result = 31 * result + (identity != null ? identity.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ZMTPGreeting{" +
           "revision=" + revision +
           ", socketType=" + socketType +
           ", identity=" + identity +
           '}';
  }
}
