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

import java.io.Closeable;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * A streaming decoder that takes parsed ZMTP frame headers and raw content and (optionally)
 * produces some output.
 */
public interface ZMTPDecoder extends Closeable {

  /**
   * Start a new ZMTP frame.
   *
   * @param ctx    The {@link ChannelHandlerContext} where this decoder is used.
   * @param length The total length in bytes of the frame content.
   * @param more   {@code true} if there are additional frames following this one in the current
   *               ZMTP message, {@code false otherwise.}
   * @param out    {@link List} to which decoded messages should be added.
   */
  void header(final ChannelHandlerContext ctx, final long length, boolean more,
              final List<Object> out);

  /**
   * Read ZMTP frame content. Called repeatedly, at least once, per frame until all of the frame
   * content data has been read.
   *
   * @param ctx  The {@link ChannelHandlerContext} where this decoder is used.
   * @param data The raw ZMTP frame content.
   * @param out  {@link List} to which decoded messages should be added.
   */
  void content(final ChannelHandlerContext ctx, ByteBuf data, final List<Object> out);

  /**
   * End the ZMTP message. Called once after {@link #header} has been called with {@code more ==
   * false}.
   *
   * @param ctx The {@link ChannelHandlerContext} where this decoder is used.
   * @param out {@link List} to which decoded messages should be added.
   */
  void finish(final ChannelHandlerContext ctx, final List<Object> out);

  /**
   * Tear down the decoder and release e.g. retained {@link ByteBuf}s. May be called mid-message.
   */
  @Override
  void close();

  /**
   * Creates {@link ZMTPDecoder} instances.
   */
  interface Factory {

    /**
     * Create a {@link ZMTPDecoder} for a {@link ZMTPSession};
     */
    ZMTPDecoder decoder(ZMTPSession session);
  }
}
