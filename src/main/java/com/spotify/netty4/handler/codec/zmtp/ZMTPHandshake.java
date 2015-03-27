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

public class ZMTPHandshake {

  private final int version;
  private final ByteBuffer remoteIdentity;

  public ZMTPHandshake(final int version, final ByteBuffer remoteIdentity) {
    this.version = version;
    this.remoteIdentity = remoteIdentity;
  }

  public int protocolVersion() {
    return version;
  }

  public ByteBuffer remoteIdentity() {
    return remoteIdentity == null ? null : remoteIdentity.asReadOnlyBuffer();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPHandshake that = (ZMTPHandshake) o;

    if (version != that.version) { return false; }
    if (remoteIdentity != null ? !remoteIdentity.equals(that.remoteIdentity)
                               : that.remoteIdentity != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = version;
    result = 31 * result + (remoteIdentity != null ? remoteIdentity.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ZMTPHandshake{" +
           "version=" + version +
           ", remoteIdentity=" + remoteIdentity +
           '}';
  }

  public static ZMTPHandshake of(final int protocolVersion, final ByteBuffer remoteIdentity) {
    return new ZMTPHandshake(protocolVersion, remoteIdentity);
  }
}
