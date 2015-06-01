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

class ZMTPWireFormats {

  static ZMTPWireFormat wireFormat(final ZMTPVersion version) {
    switch (version) {
      case ZMTP10:
        return new ZMTP10WireFormat();
      case ZMTP20:
        return new ZMTP20WireFormat();
      default:
        throw new IllegalArgumentException("Unsupported version: " + version);
    }
  }
}
