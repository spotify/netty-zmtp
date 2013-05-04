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
import java.util.UUID;

/**
 * Represents a ongoing zmtp session
 */
public class ZMTPSession {

  private final boolean useLocalIdentity;
  private final byte[] localIdent;

  private ZMTPConnectionType type;
  private Channel channel;
  private byte[] remoteIdent;

  public ZMTPSession(final ZMTPConnectionType type) {
    this(type, null);
  }

  public ZMTPSession(final ZMTPConnectionType type, @Nullable final byte[] localIdent) {
    this.type = type;
    this.useLocalIdentity = (localIdent != null);
    if (localIdent == null) {
      this.localIdent = ZMTPUtils.getBytesFromUUID(UUID.randomUUID());
    } else {
      this.localIdent = localIdent;
    }
  }

  /**
   * @return The local address of the session
   */
  public SocketAddress getLocalAddress() {
    return channel.getLocalAddress();
  }

  /**
   * @return The remote address of the session
   */
  public SocketAddress getRemoteAddress() {
    return channel.getRemoteAddress();
  }

  /**
   * Type of connection dictates if a identity frame is needed
   *
   * @return Returns the type of connection
   */
  public ZMTPConnectionType getConnectionType() {
    return type;
  }

  /**
   * Changes the type of connection of the session
   */
  void setConnectionType(final ZMTPConnectionType type) {
    this.type = type;
  }

  /**
   * Helper to determine if messages in this session are enveloped
   */
  public boolean isEnveloped() {
    return (type == ZMTPConnectionType.Addressed);
  }

  /**
   * Get the remote session id (can be used for persistent queuing)
   */
  public byte[] getRemoteIdentity() {
    return remoteIdent;
  }

  /**
   * Return the local identity
   */
  public byte[] getLocalIdentity() {
    return localIdent;
  }

  /**
   * Do we have a local identity or does the system create one
   */
  public boolean useLocalIdentity() {
    return useLocalIdentity;
  }

  /**
   * Set the remote identity
   *
   * @param remoteIdent Remote identity, if null an identity will be created
   */
  public void setRemoteIdentity(@Nullable final byte[] remoteIdent) throws ZMTPException {
    if (this.remoteIdent != null) {
      throw new ZMTPException("Remote identity already set");
    }

    this.remoteIdent = remoteIdent;
    if (this.remoteIdent == null) {
      // Create a new remote identity
      this.remoteIdent = ZMTPUtils.getBytesFromUUID(UUID.randomUUID());
    }
  }

  public Channel getChannel() {
    return channel;
  }

  public void setChannel(final Channel channel) {
    this.channel = channel;
  }
}
