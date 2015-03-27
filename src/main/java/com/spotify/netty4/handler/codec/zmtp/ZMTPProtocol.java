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

public interface ZMTPProtocol {

  ZMTPHandshaker handshaker(ZMTPSession session);

  ZMTPVersion version();

  static class ZMTP10 {

    public static ZMTP10Protocol.Builder builder() {
      return ZMTP10Protocol.builder();
    }

    public static ZMTP10Protocol withConnectionType(ZMTPConnectionType connectionType) {
      return ZMTP10Protocol.builder().connectionType(connectionType).build();
    }
  }

  static class ZMTP20 {

    public static ZMTP20Protocol.Builder builder() {
      return ZMTP20Protocol.builder();
    }

    public static ZMTP20Protocol withSocketType(ZMTPSocketType socketType) {
      return ZMTP20Protocol.builder().socketType(socketType).build();
    }
  }
}
