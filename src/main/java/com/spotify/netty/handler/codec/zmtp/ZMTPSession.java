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

import org.jboss.netty.channel.Channel;
import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.UUID;

/**
 * Represents a ongoing zmtp session
 */
public class ZMTPSession {

  public static final int DEFAULT_SIZE_LIMIT = Integer.MAX_VALUE;

  private final byte[] localIdent;
  private final long sizeLimit;
  private final ZMTPSocketType socketType;

  private Channel channel;
  private byte[] remoteIdent;
  private volatile int actualVersion;

  public ZMTPSession() {
    this(Integer.MAX_VALUE);
  }

  public ZMTPSession(final long sizeLimit) {
    this(sizeLimit, null, null);
  }

  public ZMTPSession(@Nullable final byte[] localIdent) {
    this(DEFAULT_SIZE_LIMIT, localIdent, null);
  }

  public ZMTPSession(final long sizeLimit,
                     @Nullable final ZMTPSocketType socketType) {
    this(sizeLimit, null, socketType);
  }

  public ZMTPSession(final long sizeLimit,
                     @Nullable final byte[] localIdent,
                     @Nullable final ZMTPSocketType socketType) {
    this.sizeLimit = sizeLimit;
    if (localIdent == null) {
      this.localIdent = ZMTPUtils.getBytesFromUUID(UUID.randomUUID());
    } else {
      this.localIdent = localIdent;
    }
    this.socketType = socketType;
  }

  /**
   * @return The local address of the session
   */
  public SocketAddress localAddress() {
    return channel.getLocalAddress();
  }

  /**
   * @return The remote address of the session
   */
  public SocketAddress remoteAddress() {
    return channel.getRemoteAddress();
  }

  /**
   * Get the remote session id (can be used for persistent queuing)
   */
  public byte[] remoteIdentity() {
    return remoteIdent;
  }

  /**
   * Return the local identity
   */
  public byte[] localIdentity() {
    return localIdent;
  }

  /**
   * Set the remote identity
   *
   * @param remoteIdent Remote identity, if null an identity will be created
   */
  public void remoteIdentity(@Nullable final byte[] remoteIdent) {
    if (this.remoteIdent != null) {
      throw new IllegalStateException("Remote identity already set");
    }

    this.remoteIdent = remoteIdent;
    if (this.remoteIdent == null) {
      // Create a new remote identity
      this.remoteIdent = ZMTPUtils.getBytesFromUUID(UUID.randomUUID());
    }
  }

  public Channel channel() {
    return channel;
  }

  public void channel(final Channel channel) {
    this.channel = channel;
  }


  public long sizeLimit() {
    return sizeLimit;
  }

  /**
   * An integer representing the actual version of an ZMTP connection. Note that this property
   * does not effect which protcol versions support (To pick a version, select one of the CodecBase
   * subclasses) and the actualVersion might be lower than the highest supported version of your
   * CodecBase subclass due to interoperability downgrades.
   *
   * @return 1 for ZMTP/1.0 or 2 for ZMTP/2.0.
   */
  public int actualVersion() {
    return actualVersion;
  }

  public void actualVersion(int actualVersion) {
      this.actualVersion = actualVersion;
  }

  @Nullable
  public ZMTPSocketType socketType() {
    return socketType;
  }

  @Override
  public String toString() {
    return "ZMTPSession{" +
           ", localIdent=" + Arrays.toString(localIdent) +
           ", sizeLimit=" + sizeLimit +
           ", socketType=" + socketType +
           ", channel=" + channel +
           ", remoteIdent=" + Arrays.toString(remoteIdent) +
           ", actualVersion=" + actualVersion +
           '}';
  }
}
