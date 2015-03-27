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

import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP20;

public class ZMTP20Protocol implements ZMTPProtocol {

  private final ZMTPSocketType socketType;
  private final boolean interop;

  private ZMTP20Protocol(final Builder builder) {
    this.socketType = builder.socketType;
    this.interop = builder.interop;
  }

  public ZMTPSocketType socketType() {
    return socketType;
  }

  public boolean interop() {
    return interop;
  }

  @Override
  public ZMTPHandshaker handshaker(final ZMTPSession session) {
    return new ZMTP20Handshaker(socketType, interop, session.localIdentity());
  }

  @Override
  public ZMTPVersion version() {
    return ZMTP20;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private ZMTPSocketType socketType;
    private boolean interop = true;

    public Builder socketType(final ZMTPSocketType socketType) {
      this.socketType = socketType;
      return this;
    }

    public Builder interop(final boolean interop) {
      this.interop = interop;
      return this;
    }

    public ZMTP20Protocol build() {
      return new ZMTP20Protocol(this);
    }
  }
}
