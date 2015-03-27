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

public class ZMTP20Protocol implements ZMTPProtocol {

  public static final boolean DEFAULT_INTEROP = true;

  private final boolean interop;

  public ZMTP20Protocol() {
    this(DEFAULT_INTEROP);
  }

  public ZMTP20Protocol(final boolean interop) {
    this.interop = interop;
  }

  @Override
  public ZMTP20Handshaker handshaker(final ZMTPSession session) {
    return new ZMTP20Handshaker(session.socketType(), interop, session.localIdentity());
  }

  public ZMTP20Protocol withInterop(final boolean interop) {
    return new ZMTP20Protocol(interop);
  }

  public ZMTP20Protocol withInterop() {
    return withInterop(true);
  }

  public ZMTP20Protocol withoutInterop() {
    return withInterop(false);
  }
}
