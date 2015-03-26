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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.netty4.handler.codec.zmtp.VerifyingDecoder.ExpectedOutput;

import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;

/**
 * This test attempts to thoroughly exercise the {@link ZMTPMessageParser} by feeding it input
 * fragmented in every possible way using {@link Fragmenter}. Everything from whole un-fragmented
 * message parsing to each byte being fragmented in a separate buffer is tested. Generating all
 * possible message fragmentations takes some time, so running this test can typically take a few
 * minutes.
 */
@RunWith(Theories.class)
public class ZMTPMessageParserTest {

  @DataPoints
  public static String[][] FRAMES = {
      {"1"},
      {"2", ""},
      {"3", "aa"},
      {"4", "", "a"},
      {"5", "", "a", "bb"},
      {"6", "aa", "", "b", "cc"},
      {"7", "", "a"},
      {"8", "", "b", "cc"},
      {"9", "aa", "", "b", "cc"},
  };

  @Theory
  public void testParse(final String[] frames) throws Exception {
    for (final int version : asList(1, 2)) {
      testParse(asList(frames), version);
    }
  }

  private void testParse(final List<String> input, final int version)
      throws Exception {
    System.out.printf("version=%d, input=%s%n", version, input);

    final ExpectedOutput expected = new ExpectedOutput(frames(input));

    final ByteBuf serialized = serialize(input, version);
    final int serializedLength = serialized.readableBytes();

    // Test parsing the whole message
    {
      final VerifyingDecoder verifier = new VerifyingDecoder(expected);
      final ZMTPMessageParser parser = ZMTPMessageParser.create(version, verifier);
      parser.parse(serialized, null);
      verifier.assertFinished();
      serialized.setIndex(0, serializedLength);
    }

    // Prepare for trivial message parsing test
    final List<String> envelope = asList("e", "");
    final List<String> content = asList("a", "b", "c");
    final List<String> frames = newArrayList(concat(envelope, content));
    final ByteBuf trivialSerialized = serialize(frames, version);
    final int trivialLength = trivialSerialized.readableBytes();
    final ExpectedOutput trivialExpected = new ExpectedOutput(frames(
        Lists.newArrayList(concat(envelope, content))));

    // Test parsing fragmented input
    final VerifyingDecoder verifier = new VerifyingDecoder();
    final ZMTPMessageParser parser = ZMTPMessageParser.create(version, verifier);
    new Fragmenter(serialized.readableBytes()).fragment(new Fragmenter.Consumer() {
      @Override
      public void fragments(final int[] limits, final int count) throws Exception {
        verifier.expect(expected);
        serialized.setIndex(0, serializedLength);
        for (int i = 0; i < count; i++) {
          final int limit = limits[i];
          serialized.writerIndex(limit);
          parser.parse(serialized, null);
        }
        verifier.assertFinished();

        // Verify that the parser can be reused to parse the same message
        serialized.setIndex(0, serializedLength);
        parser.parse(serialized, null);
        verifier.assertFinished();

        // Verify that the parser can be reused to parse a well-behaved message
        verifier.expect(trivialExpected);
        trivialSerialized.setIndex(0, trivialLength);
        parser.parse(trivialSerialized, null);
        verifier.assertFinished();
      }
    });
  }

  public static List<ByteBuf> frames(final String... frames) {
    return frames(asList(frames));
  }

  public static List<ByteBuf> frames(final List<String> frames) {
    final ImmutableList.Builder<ByteBuf> zmtpFrames = ImmutableList.builder();
    for (final String frame : frames) {
      zmtpFrames.add(Unpooled.copiedBuffer(frame, UTF_8));
    }
    return zmtpFrames.build();
  }

  private ByteBuf serialize(final List<String> frames, int version) {
    final ZMTPMessage message = ZMTPMessage.fromStringsUTF8(false, frames);
    final int messageSize = ZMTPUtils.messageSize(message, false, version);
    final ByteBuf buffer = Unpooled.buffer(messageSize);
    ZMTPUtils.writeMessage(message, buffer, false, version);
    return buffer;
  }
}
