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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessage.fromStringsUTF8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ZMTPMessageTest {

  @Test
  public void testFromStringsUTF8() {
    assertEquals(fromStringsUTF8(true, ""), message(frames(), frames()));
    assertEquals(fromStringsUTF8(true, "e", ""), message(frames("e"), frames()));
    assertEquals(fromStringsUTF8(true, "e", "", "c"), message(frames("e"), frames("c")));
    assertEquals(fromStringsUTF8(true, "e", "", "c1", "c2"), message(frames("e"), frames("c1", "c2")));
    assertEquals(fromStringsUTF8(true, "c"), message(frames(), frames("c")));
    assertEquals(fromStringsUTF8(true, "c1", "c2"), message(frames(), frames("c1", "c2")));

    assertEquals(fromStringsUTF8(false, ""), message(frames(), frames("")));
    assertEquals(fromStringsUTF8(false, "e", ""), message(frames(), frames("e", "")));
    assertEquals(fromStringsUTF8(false, "e", "", "c"), message(frames(), frames("e", "", "c")));
    assertEquals(fromStringsUTF8(false, "e", "", "c1", "c2"), message(frames(), frames("e", "", "c1", "c2")));
    assertEquals(fromStringsUTF8(false, "c"), message(frames(), frames("c")));
    assertEquals(fromStringsUTF8(false, "c1", "c2"), message(frames(), frames("c1", "c2")));
  }

  private ZMTPMessage message(final List<ZMTPFrame> envelope, final List<ZMTPFrame> content) {
    return new ZMTPMessage(envelope, content);
  }

  public static List<ZMTPFrame> frames(final String... frames) {
    return frames(asList(frames));
  }

  private static List<ZMTPFrame> frames(final List<String> frames) {
    return Lists.transform(frames, new Function<String, ZMTPFrame>() {
      @Override
      public ZMTPFrame apply(final String input) {
        return ZMTPFrame.from(input);
      }
    });
  }
}
