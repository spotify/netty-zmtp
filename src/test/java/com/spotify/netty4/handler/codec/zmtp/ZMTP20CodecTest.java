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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPConnectionType.Addressed;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;


public class ZMTP20CodecTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void verifyRequiresSocketType() {
    final ZMTPSocketType socketType = null;
    final ZMTPSession session = new ZMTPSession(Addressed, 1024, socketType);
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("ZMTP/2.0 requires a socket type");
    new ZMTP20Codec(session, true);
  }

  @Test
  public void testConstruction() {
    final ZMTPSession session = new ZMTPSession(Addressed, 1024, DEALER);
    new ZMTP20Codec(session, true);
  }
}