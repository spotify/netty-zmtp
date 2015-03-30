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

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public enum ZMTPVersion {
  ZMTP10(0, true),
  ZMTP20(1, true);

  private final int revision;
  private final boolean supported;

  ZMTPVersion(final int revision, final boolean supported) {
    this.revision = revision;
    this.supported = supported;
  }

  public int revision() {
    return revision;
  }

  public boolean supported() {
    return supported;
  }

  private static final List<ZMTPVersion> SUPPORTED = unmodifiableList(asList(ZMTP10, ZMTP20));

  public static boolean isSupported(final ZMTPVersion version) {
    return SUPPORTED.contains(version);
  }

  public static boolean isSupported(final int revision) {
    for (final ZMTPVersion version : SUPPORTED) {
      if (version.revision == revision) {
        return version.supported;
      }
    }
    return false;
  }

  public static List<ZMTPVersion> supportedVersions() {
    return SUPPORTED;
  }
}
