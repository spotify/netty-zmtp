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

import java.nio.ByteBuffer;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;

/**
 * Represents one end of a single ZMTP connection.
 */
public class ZMTPSession {

  private final DefaultPromise<ZMTPHandshake> handshake = new DefaultPromise<ZMTPHandshake>(
      GlobalEventExecutor.INSTANCE);

  private final ZMTPConfig config;

  private volatile ByteBuffer peerIdentity;

  ZMTPSession(final ZMTPConfig config) {
    this.config = checkNotNull(config, "config");
  }

  /**
   * The configuration of this ZMTP session.
   */
  public ZMTPConfig config() {
    return config;
  }

  /**
   * Get the peer identity.
   */
  public ByteBuffer peerIdentity() {
    if (!handshake.isDone()) {
      throw new IllegalStateException("handshake not complete");
    }
    return peerIdentity.asReadOnlyBuffer();
  }

  /**
   * Check whether the peer is anonymous and the peer identity is generated.
   */
  public boolean isPeerAnonymous() {
    return !handshake().remoteIdentity().hasRemaining();
  }

  /**
   * The ZMTP framing version negotiated as part of the handshake on connection establishment.
   */
  public ZMTPVersion negotiatedVersion() {
    return handshake().negotiatedVersion();
  }

  /**
   * Get a future that will be notified when the ZMTP handshake is complete.
   */
  public Future<ZMTPHandshake> handshakeFuture() {
    return handshake;
  }

  /**
   * Signal ZMTP handshake success.
   */
  void handshakeSuccess(final ZMTPHandshake handshake) {
    peerIdentity = handshake.remoteIdentity().hasRemaining()
                   ? handshake.remoteIdentity()
                   : config.identityGenerator().generateIdentity(this);
    this.handshake.setSuccess(handshake);
  }

  /**
   * Signal ZMTP handshake failure.
   */
  void handshakeFailure(final Throwable cause) {
    this.handshake.tryFailure(cause);
  }

  private ZMTPHandshake handshake() {
    if (!handshake.isDone()) {
      throw new IllegalStateException("handshake not complete");
    }
    final ZMTPHandshake handshake = this.handshake.getNow();
    assert handshake != null;
    return handshake;
  }

  @Override
  public String toString() {
    return "ZMTPSession{" +
           "config=" + config +
           ", handshake=" + handshake +
           '}';
  }

  public static ZMTPSession from(final ZMTPConfig config) {
    return new ZMTPSession(config);
  }
}
