/*
 * Copyright (c) 2012-2014 Spotify AB
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

import java.io.Closeable;

import io.netty.buffer.ByteBuf;

/**
 * An encoder that takes implementation defined messages and writes a stream of ZMTP frames.
 */
public interface ZMTPEncoder extends Closeable {

  /**
   * Estimate ZMTP output for the {@code message} using a {@link ZMTPEstimator}. Called before
   * {@link #encode}.
   *
   * @param message   The message to be estimated.
   * @param estimator The {@link ZMTPEstimator} to use.
   */
  void estimate(Object message, ZMTPEstimator estimator);

  /**
   * Write ZMTP output for the {@code message} using the {@link ZMTPWriter}. Called after {@link
   * #estimate}.
   *
   * @param message The message to write.
   * @param writer  The {@link ZMTPWriter} to use.
   */
  void encode(Object message, ZMTPWriter writer);

  /**
   * Tear down the encoder and release e.g. retained {@link ByteBuf}s.
   */
  @Override
  void close();

  /**
   * Creates {@link ZMTPEncoder} instances.
   */
  interface Factory {

    /**
     * Create a {@link ZMTPEncoder} for a {@link ZMTPSession};
     */
    ZMTPEncoder encoder(ZMTPSession session);
  }
}
