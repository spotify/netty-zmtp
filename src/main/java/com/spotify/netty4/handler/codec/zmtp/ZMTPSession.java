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
import java.util.concurrent.atomic.AtomicReference;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;

/**
 * Represents an ongoing zmtp session
 */
public class ZMTPSession {

  private final AtomicReference<ZMTPHandshake> handshake = new AtomicReference<ZMTPHandshake>();

  private final ZMTPConfig config;

  ZMTPSession(final ZMTPConfig config) {
    this.config = checkNotNull(config, "config");
  }

  /**
   * The the configuration of this ZMTP session.
   */
  public ZMTPConfig config() {
    return config;
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
   * Signal ZMTP handshake completion.
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

  @Override
  public String toString() {
    return "ZMTPSession{" +
           "config=" + config +
           ", handshake=" + handshake +
           '}';
  }
}
