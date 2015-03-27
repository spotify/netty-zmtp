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

import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents an ongoing zmtp session
 */
public class ZMTPSession {

  private final ByteBuffer localIdentity;
  private final ZMTPSocketType socketType;
  private final ZMTPConnectionType type;

  private final boolean useLocalIdentity;

  private AtomicReference<ZMTPHandshake> handshake = new AtomicReference<ZMTPHandshake>();

  private ZMTPSession(final Builder builder) {
    this.type = builder.type;
    this.useLocalIdentity = (builder.localIdentity != null);
    if (builder.localIdentity == null) {
      this.localIdentity = ByteBuffer.wrap(ZMTPUtils.encodeUUID(UUID.randomUUID()));
    } else {
      this.localIdentity = builder.localIdentity;
    }
    this.socketType = builder.socketType;
  }

  /**
   * Type of connection dictates if a identity frame is needed
   *
   * @return Returns the type of connection
   */
  public ZMTPConnectionType connectionType() {
    return type;
  }

  /**
   * Helper to determine if messages in this session are enveloped
   */
  public boolean isEnveloped() {
    return (type == ZMTPConnectionType.ADDRESSED);
  }

  /**
   * Get the remote session id (can be used for persistent queuing)
   */
  public ByteBuffer remoteIdentity() {
    final ZMTPHandshake handshake = this.handshake.get();
    if (handshake == null) {
      throw new IllegalStateException("handshake not complete");
    }
    return handshake.remoteIdentity();
  }

  /**
   * Return the local identity
   */
  public ByteBuffer localIdentity() {
    return localIdentity.asReadOnlyBuffer();
  }

  /**
   * Do we have a local identity or does the system create one
   */
  public boolean useLocalIdentity() {
    return useLocalIdentity;
  }

  /**
   * Set the
   */
  void handshakeDone(final ZMTPHandshake handshake) {
    if (!this.handshake.compareAndSet(null, handshake)) {
      throw new IllegalStateException("handshake result already set");
    }
  }

  /**
   * An integer representing the actual version of an ZMTP connection. Note that this property does
   * not effect which protocol versions support (To pick a version, select one of the CodecBase
   * subclasses) and the actualVersion might be lower than the highest supported version of your
   * CodecBase subclass due to interoperability downgrades.
   *
   * @return 1 for ZMTP/1.0 or 2 for ZMTP/2.0.
   */
  public int actualVersion() {
    final ZMTPHandshake handshake = this.handshake.get();
    if (handshake == null) {
      throw new IllegalStateException("handshake not complete");
    }
    return handshake.protocolVersion();
  }

  @Nullable
  public ZMTPSocketType socketType() {
    return socketType;
  }

  @Override
  public String toString() {
    return "ZMTPSession{" +
           "localIdentity=" + localIdentity +
           ", socketType=" + socketType +
           ", type=" + type +
           ", useLocalIdentity=" + useLocalIdentity +
           ", handshake=" + handshake +
           '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private ByteBuffer localIdentity;
    private ZMTPSocketType socketType;
    private ZMTPConnectionType type;

    public Builder localIdentity(final byte[] localIdentity) {
      return localIdentity(ByteBuffer.wrap(localIdentity));
    }

    public Builder localIdentity(final ByteBuffer localIdentity) {
      this.localIdentity = localIdentity;
      return this;
    }

    public Builder socketType(final ZMTPSocketType socketType) {
      this.socketType = socketType;
      return this;
    }

    public Builder type(final ZMTPConnectionType type) {
      this.type = type;
      return this;
    }

    public ZMTPSession build() {
      return new ZMTPSession(this);
    }
  }
}
