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
    assertEquals(fromStringsUTF8(""), message(""));
    assertEquals(fromStringsUTF8("a"), message("a"));
    assertEquals(fromStringsUTF8("aa"), message("aa"));
    assertEquals(fromStringsUTF8("aa", "bb"), message("aa", "bb"));
    assertEquals(fromStringsUTF8("aa", "", "bb"), message("aa", "", "bb"));
  }

  private ZMTPMessage message(final String... frames) {
    return new ZMTPMessage(frames(asList(frames)));
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
