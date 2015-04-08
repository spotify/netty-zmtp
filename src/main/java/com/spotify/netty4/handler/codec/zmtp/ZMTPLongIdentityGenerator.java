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

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ZMTPIdentityGenerator} that generates identities using a {@code long} counter.
 */
public class ZMTPLongIdentityGenerator implements ZMTPIdentityGenerator {

  public static ZMTPLongIdentityGenerator GLOBAL = new ZMTPLongIdentityGenerator();

  private static final AtomicLong peerIdCounter = new AtomicLong(new SecureRandom().nextLong());

  @Override
  public ByteBuffer generateIdentity(final ZMTPSession session) {
    final ByteBuffer generated = ByteBuffer.allocate(9);
    generated.put((byte) 0);
    generated.putLong(peerIdCounter.incrementAndGet());
    generated.flip();
    return generated;
  }
}
