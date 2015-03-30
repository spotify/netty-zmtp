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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

interface ZMTPHandshaker {

  /**
   * Get a greeting to send immediately when a connection is established.
   */
  ByteBuf greeting();

  /**
   * Continue handshake in response to receiving data from the remote peer. This method is called
   * repeatedly until it returns a non-null {@link ZMTPHandshake} result.
   *
   * @param in  Data from the remote peer.
   * @param ctx The channel handler context.
   * @return A {@link ZMTPHandshake} if the handshake is complete, null otherwise.
   * @throws ZMTPException for protocol errors.
   */
  ZMTPHandshake handshake(ByteBuf in, ChannelHandlerContext ctx) throws ZMTPException;
}
