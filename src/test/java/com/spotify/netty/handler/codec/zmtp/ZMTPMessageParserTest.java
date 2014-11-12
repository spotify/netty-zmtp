/*
 * Copyright (c) 2012-2013 Spotify AB
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

package com.spotify.netty.handler.codec.zmtp;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Limit.limit;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Limit.unlimited;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.expectation;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.input;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.test;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.truncated;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.whole;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.out;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * This test attempts to thoroughly exercise the {@link ZMTPMessageParser} by feeding it input
 * fragmented in every possible way using {@link Fragmenter}. Everything from whole un-fragmented
 * message parsing to each byte being fragmented in a separate buffer is tested. Generating all
 * possible message fragmentations takes some time, so running this test can typically take a few
 * minutes.
 */
@RunWith(Theories.class)
public class ZMTPMessageParserTest {

  public static final int CPUS = Runtime.getRuntime().availableProcessors();
  public static final ExecutorService EXECUTOR = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(CPUS), 0, SECONDS);

  @Test
  public void testZMTP1LongFrameSize() throws ZMTPMessageParsingException {
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    buffer.writeByte(0xFF);
    ZMTPMessageParser parser = new ZMTPMessageParser(1024, 1);
    ZMTPParsedMessage msg = parser.parse(buffer);
    assertNull("Message shouldn't be parsed for missing frame size",
               msg);
  }

  @Test
  public void testZMTP1BufferLengthEmpty() throws ZMTPMessageParsingException {
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    ZMTPMessageParser parser = new ZMTPMessageParser(1024, 1);
    ZMTPParsedMessage msg = parser.parse(buffer);
    assertNull("Empty ChannelBuffer should result in an empty ZMTPParsedMessage",
               msg);
  }

  @DataPoints
  public static Parameters[] PARAMETERS = {
      test(input("1"), expectation("1")),
      test(input("2", ""), expectation("2", "")),
      test(input("3", "aa"), expectation("3", "aa")),
      test(input("4", "", "a"), expectation("4", "", "a")),
      test(input("5", "", "a", "bb"), expectation("5", "", "a", "bb")),
      test(input("6", "aa", "", "b", "cc"), expectation("6", "aa", "", "b", "cc")),
      test(input("7", "", "a"), expectation("7", "", "a")),
      test(input("8", "", "b", "cc"), expectation("8", "", "b", "cc")),
      test(input("9", "aa", "", "b", "cc"), expectation("9", "aa", "", "b", "cc")),

      test(input("a", "bb", "", "c", "dd", "", "eee"),
           expectation(limit(1), truncated(9, "a")),
           expectation(limit(2), truncated(9, "a")),
           expectation(limit(3), truncated(9, "a", "bb", "")),
           expectation(limit(4), truncated(9, "a", "bb", "", "c")),
           expectation(limit(5), truncated(9, "a", "bb", "", "c")),
           expectation(limit(6), truncated(9, "a", "bb", "", "c", "dd", "")),
           expectation(limit(7), truncated(9, "a", "bb", "", "c", "dd", "")),
           expectation(limit(8), truncated(9, "a", "bb", "", "c", "dd", "")),
           expectation(limit(9), whole(9, "a", "bb", "", "c", "dd", "", "eee"))
      ),
  };

  @Theory
  public void testParse(final Parameters parameters) throws Exception {
    final List<Future<?>> futures = newArrayList();
    for (final Verification v : parameters.verifications()) {
      futures.add(EXECUTOR.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          testParse(v.sizeLimit, v.inputFrames, v.expectedMessage, 1);
          testParse(v.sizeLimit, v.inputFrames, v.expectedMessage, 2);
          return null;
        }
      }));
    }
    for (final Future<?> future : futures) {
      Uninterruptibles.getUninterruptibly(future);
    }
  }

  private void testParse(final Limit limit, final List<String> input,
                         final ZMTPParsedMessage expected, int version) throws Exception {
    out.println(format("limit=%s input=%s expected=%s",
                       limit, input, expected));

    final ChannelBuffer serialized = serialize(input, version);
    final int serializedLength = serialized.readableBytes();

    // Test parsing the whole message
    {
      final ZMTPMessageParser parser = new ZMTPMessageParser(limit.value, 1);
      final ZMTPParsedMessage parsed = parser.parse(serialized);
      serialized.setIndex(0, serializedLength);
      assertEquals("expected: " + expected + ", parsed: " + parsed, expected, parsed);
    }

    // Prepare for trivial message parsing test
    final int contentSize = min(limit.value - 1, 10);
    final List<String> envelope = asList("e", "");
    final List<String> content = nCopies(contentSize, ".");
    final List<String> frames = newArrayList(concat(envelope, content));
    final ZMTPMessage trivialMessage = ZMTPMessage.fromStringsUTF8(frames);
    final ChannelBuffer trivialSerialized = serialize(frames, version);
    final int trivialLength = trivialSerialized.readableBytes();

    // Test parsing fragmented input
    new Fragmenter(serialized.readableBytes()).fragment(new Fragmenter.Consumer() {
      @Override
      public void fragments(final int[] limits, final int count) throws Exception {
        serialized.setIndex(0, serializedLength);
        ZMTPParsedMessage parsed = null;
        final ZMTPMessageParser parser = new ZMTPMessageParser(limit.value, 1);
        for (int i = 0; i < count; i++) {
          final int limit = limits[i];
          serialized.writerIndex(limit);
          parsed = parser.parse(serialized);
          // Verify that the parser did not return a message for incomplete input
          if (limit < serializedLength) {
            assertNull(parsed);
          }
        }
        assertEquals(expected, parsed);

        // Verify that the parser can be reused to parse the same message
        serialized.setIndex(0, serializedLength);
        final ZMTPParsedMessage reparsed = parser.parse(serialized);
        assertEquals(expected, reparsed);

        // Verify that the parser can be reused to parse a well-behaved message
        trivialSerialized.setIndex(0, trivialLength);
        final ZMTPParsedMessage parsedTrivial = parser.parse(trivialSerialized);
        assertFalse(parsedTrivial.isTruncated());
        assertEquals(trivialMessage, parsedTrivial.getMessage());
      }
    });

  }

  static class Parameters {

    final List<String> inputFrames;
    final List<Verification> verifications;

    Parameters(final List<String> inputFrames,
               final List<Verification> verifications) {
      this.inputFrames = inputFrames;
      this.verifications = verifications;
    }

    public static Parameters test(final List<String> input, final Expectation... expectations) {
      final List<Verification> verifications = newArrayList();
      for (final Expectation expectation : expectations) {
        verifications.add(verification(input, expectation));
      }
      return new Parameters(input, verifications);
    }

    private static Verification verification(final List<String> input,
                                             final Expectation e) {
      final ZMTPParsedMessage expected;
      if (e.expected == null) {
        final ZMTPMessage message = ZMTPMessage.fromStringsUTF8(input);
        expected = new ZMTPParsedMessage(false, byteSizeUTF8(input), message);
      } else {
        expected = e.expected;
      }
      return new Verification(e.sizeLimit, input, expected);
    }

    private static long byteSizeUTF8(final List<String> frames) {
      long size = 0;
      for (final String frame : frames) {
        size += byteSize(ZMTPFrame.create(frame));
      }
      return size;
    }

    private static long byteSize(final List<ZMTPFrame> frames) {
      long size = 0;
      for (final ZMTPFrame frame : frames) {
        size += byteSize(frame);
      }
      return size;
    }

    private static long byteSize(final ZMTPFrame frame) {
      return frame.size();
    }

    public static Expectation expectation(final Limit sizeLimit, final Output o) {
      final ZMTPMessage message = new ZMTPMessage(o.frames);
      final ZMTPParsedMessage expected = new ZMTPParsedMessage(o.truncated, o.byteSize, message);
      return expectation(sizeLimit, expected);
    }

    public static Expectation expectation(final String... frames) {
      return expectation(frames(frames));
    }

    public static Expectation expectation(final List<ZMTPFrame> frames) {
      return expectation(unlimited(), new ZMTPParsedMessage(false, byteSize(frames), new ZMTPMessage(frames)));
    }

    private static Expectation expectation(final Limit limit, final ZMTPParsedMessage message) {
      return new Expectation(limit, message);
    }

    public static List<String> input(final String... frames) {
      return asList(frames);
    }

    public static List<ZMTPFrame> frames(final String... frames) {
      return frames(asList(frames));
    }

    private static List<ZMTPFrame> frames(final List<String> frames) {
      return Lists.transform(frames, new Function<String, ZMTPFrame>() {
        @Override
        public ZMTPFrame apply(final String input) {
          return ZMTPFrame.create(input);
        }
      });
    }

    public static Output whole(final long byteSize, final String... frames) {
      return output(false, byteSize, frames(frames));
    }

    public static Output truncated(final long byteSize,
                                   final String... frames) {
      return output(true, byteSize, frames(frames));
    }

    public static Output output(final boolean truncated, final long byteSize,
                                final List<ZMTPFrame> frames) {
      return new Output(truncated, byteSize, frames);
    }

    public List<Verification> verifications() {
      return verifications;
    }
  }

  public static ChannelBuffer serialize(final ZMTPMessage message, int version) {
    final ChannelBuffer buffer = ChannelBuffers.buffer(ZMTPUtils.messageSize(message, version));
    ZMTPUtils.writeMessage(message, buffer, 1);
    return buffer;
  }

  private ChannelBuffer serialize(final List<String> frames, int version) {
    return serialize(ZMTPMessage.fromStringsUTF8(frames), version);
  }

  static class Verification {

    private final Limit sizeLimit;
    private final List<String> inputFrames;
    private final ZMTPParsedMessage expectedMessage;

    public Verification(final Limit sizeLimit, final List<String> inputFrames,
                        final ZMTPParsedMessage expectedMessage) {

      this.sizeLimit = sizeLimit;
      this.inputFrames = inputFrames;
      this.expectedMessage = expectedMessage;
    }
  }

  static class Limit {

    final int value;

    Limit(final int value) {
      this.value = value;
    }

    static Limit limit(final int limit) {
      return new Limit(limit);
    }

    static Limit unlimited() {
      return new Limit(Integer.MAX_VALUE);
    }

    @Override
    public String toString() {
      return Integer.toString(value);
    }
  }

  private static class Output {

    private final boolean truncated;
    private final long byteSize;
    private final List<ZMTPFrame> frames;

    public Output(final boolean truncated, final long byteSize,
                  final List<ZMTPFrame> frames) {
      this.truncated = truncated;
      this.byteSize = byteSize;
      this.frames = frames;
    }
  }

  static class Expectation {

    final Limit sizeLimit;
    final ZMTPParsedMessage expected;

    Expectation(final Limit sizeLimit, final ZMTPParsedMessage expected) {
      this.sizeLimit = sizeLimit;
      this.expected = expected;
    }
  }
}
