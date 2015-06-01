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

import javax.annotation.Nullable;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;

public class ZMTPHandshake {

  private final ZMTPVersion negotiatedVersion;
  private final ByteBuffer remoteIdentity;
  private final ZMTPSocketType remoteSocketType;

  private ZMTPHandshake(final ZMTPVersion negotiatedVersion,
                        final ByteBuffer remoteIdentity, final ZMTPSocketType remoteSocketType) {
    this.negotiatedVersion = checkNotNull(negotiatedVersion, "negotiatedVersion");
    this.remoteIdentity = checkNotNull(remoteIdentity, "remoteIdentity");
    this.remoteSocketType = remoteSocketType;
  }

  public ZMTPVersion negotiatedVersion() {
    return negotiatedVersion;
  }

  public ByteBuffer remoteIdentity() {
    return remoteIdentity.asReadOnlyBuffer();
  }

  @Nullable
  public ZMTPSocketType remoteSocketType() {
    return remoteSocketType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    final ZMTPHandshake that = (ZMTPHandshake) o;

    if (negotiatedVersion != that.negotiatedVersion) { return false; }
    if (remoteIdentity != null ? !remoteIdentity.equals(that.remoteIdentity)
                               : that.remoteIdentity != null) { return false; }
    if (remoteSocketType != that.remoteSocketType) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    int result = negotiatedVersion != null ? negotiatedVersion.hashCode() : 0;
    result = 31 * result + (remoteSocketType != null ? remoteSocketType.hashCode() : 0);
    result = 31 * result + (remoteIdentity != null ? remoteIdentity.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ZMTPHandshake{" +
           "negotiatedVersion=" + negotiatedVersion +
           ", remoteSocketType=" + remoteSocketType +
           '}';
  }

  static ZMTPHandshake of(final ZMTPVersion negotiatedVersion,
                          final ByteBuffer remoteIdentity) {
    return new ZMTPHandshake(negotiatedVersion, remoteIdentity, null);
  }

  static ZMTPHandshake of(final ZMTPVersion negotiatedVersion,
                          final ByteBuffer remoteIdentity, final ZMTPSocketType remoteSocketType) {
    return new ZMTPHandshake(negotiatedVersion, remoteIdentity, remoteSocketType);
  }
}
