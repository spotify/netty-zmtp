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

import com.spotify.netty4.handler.codec.zmtp.VerifyingDecoder.ExpectedOutput;

import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.supportedVersions;
import static java.util.Arrays.asList;

/**
 * This test attempts to thoroughly exercise the {@link ZMTPParser} by feeding it input fragmented
 * in every possible way using {@link Fragmenter}. Everything from whole un-fragmented message
 * parsing to each byte being fragmented in a separate buffer is tested. Generating all possible
 * message fragmentations takes some time, so running this test can typically take a few minutes.
 */
@RunWith(Theories.class)
public class ZMTPParserTest {

  private final static ByteBufAllocator ALLOC = new UnpooledByteBufAllocator(false);

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
    for (final ZMTPVersion version : supportedVersions()) {
      testParse(asList(frames), version);
    }
  }

  private void testParse(final List<String> input, final ZMTPVersion version)
      throws Exception {
    System.out.printf("version=%s, input=%s%n", version, input);

    final ZMTPMessage inputMessage = ZMTPMessage.fromUTF8(ALLOC, input);

    final ExpectedOutput expected = new ExpectedOutput(inputMessage);

    final ByteBuf serialized = inputMessage.write(ALLOC, version);
    final int serializedLength = serialized.readableBytes();

    // Test parsing the whole message
    {
      final VerifyingDecoder verifier = new VerifyingDecoder(expected);
      final ZMTPParser parser = ZMTPParser.create(version, verifier);
      parser.parse(serialized, null);
      verifier.assertFinished();
      serialized.setIndex(0, serializedLength);
    }

    // Prepare for trivial message parsing test
    final List<String> envelope = asList("e", "");
    final List<String> content = asList("a", "b", "c");
    final List<String> frames = newArrayList(concat(envelope, content));
    final ZMTPMessage message = ZMTPMessage.fromUTF8(ALLOC, frames);
    final ByteBuf trivialSerialized = message.write(ALLOC, version);
    final int trivialLength = trivialSerialized.readableBytes();
    final ExpectedOutput trivialExpected = new ExpectedOutput(
        ZMTPMessage.fromUTF8(ALLOC, concat(envelope, content)));

    // Test parsing fragmented input
    final VerifyingDecoder verifier = new VerifyingDecoder();
    final ZMTPParser parser = ZMTPParser.create(version, verifier);
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

}
