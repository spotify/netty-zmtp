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

package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import com.spotify.netty4.handler.codec.zmtp.VerifyingDecoder.ExpectedOutput;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Limit.limit;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Limit.unlimited;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.expectation;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.frames;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.input;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.output;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.test;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.lang.Math.min;
import static java.lang.System.out;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
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
    ByteBuf buffer = Unpooled.buffer();
    buffer.writeByte(0xFF);
    ZMTPIncomingMessageDecoder consumer = new ZMTPIncomingMessageDecoder(true);
    ZMTPMessageParser<ZMTPIncomingMessage> parser = ZMTPMessageParser.create(1, 1024, consumer);
    ZMTPIncomingMessage msg = parser.parse(buffer);
    assertNull("Message shouldn't be parsed for missing frame size", msg);
  }

  @Test
  public void testZMTP1BufferLengthEmpty() throws ZMTPMessageParsingException {
    ByteBuf buffer = Unpooled.buffer();
    ZMTPIncomingMessageDecoder consumer = new ZMTPIncomingMessageDecoder(true);
    ZMTPMessageParser<ZMTPIncomingMessage> parser = ZMTPMessageParser.create(1, 1024, consumer);
    ZMTPIncomingMessage msg = parser.parse(buffer);
    assertNull("Empty ByteBuf should result in an empty ZMTPIncomingMessage", msg);
  }

  @DataPoints
  public static Parameters[] PARAMETERS = {
      test(input("1"), output("1")),
      test(input("2", ""), output("2", "")),
      test(input("3", "aa"), output("3", "aa")),
      test(input("4", "", "a"), output("4", "", "a")),
      test(input("5", "", "a", "bb"), output("5", "", "a", "bb")),
      test(input("6", "aa", "", "b", "cc"), output("6", "aa", "", "b", "cc")),
      test(input("7", "", "a"), output("7", "", "a")),
      test(input("8", "", "b", "cc"), output("8", "", "b", "cc")),
      test(input("9", "aa", "", "b", "cc"), output("9", "aa", "", "b", "cc")),

      test(input("a", "bb", "", "c", "dd", "", "eee"),
           expectation(limit(1), output(frames("a"), discard(2, 0, 1, 2, 0, 3))),
           expectation(limit(2), output(frames("a"), discard(2, 0, 1, 2, 0, 3))),
           expectation(limit(3), output(frames("a", "bb", ""), discard(1, 2, 0, 3))),
           expectation(limit(4), output(frames("a", "bb", "", "c"), discard(2, 0, 3))),
           expectation(limit(5), output(frames("a", "bb", "", "c"), discard(2, 0, 3))),
           expectation(limit(6), output(frames("a", "bb", "", "c", "dd", ""), discard(3))),
           expectation(limit(7), output(frames("a", "bb", "", "c", "dd", ""), discard(3))),
           expectation(limit(8), output(frames("a", "bb", "", "c", "dd", ""), discard(3))),
           expectation(limit(9), output(frames("a", "bb", "", "c", "dd", "", "eee"), discard()))
      ),
  };

  private static List<Integer> discard(final Integer... sizes) {
    return asList(sizes);
  }

  @Theory
  public void testParse(final Parameters parameters) throws Exception {
    final List<Future<?>> futures = newArrayList();
    for (final Verification v : parameters.verifications()) {
      futures.add(EXECUTOR.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          testParse(v.expectation.sizeLimit, v.input, v.expectation.output, 1);
          testParse(v.expectation.sizeLimit, v.input, v.expectation.output, 2);
          return null;
        }
      }));
    }
    for (final Future<?> future : futures) {
      Uninterruptibles.getUninterruptibly(future);
    }
  }

  private void testParse(final Limit limit, final List<String> input,
                         final ExpectedOutput expected, final int version) throws Exception {
    out.printf("version=%d, input=%s, limit=%s, output=%s%n", version, input, limit, expected);

    final ByteBuf serialized = serialize(input, version);
    final int serializedLength = serialized.readableBytes();

    // Test parsing the whole message
    {
      final VerifyingDecoder verifier = new VerifyingDecoder(expected);
      final ZMTPMessageParser<Void> parser = ZMTPMessageParser.create(
          version, limit.value, verifier);
      parser.parse(serialized);
      verifier.assertFinished();
      serialized.setIndex(0, serializedLength);
    }

    // Prepare for trivial message parsing test
    final int contentSize = min(limit.value - 1, 10);
    final List<String> envelope = asList("e", "");
    final List<String> content = nCopies(contentSize, ".");
    final List<String> frames = newArrayList(concat(envelope, content));
    final ByteBuf trivialSerialized = serialize(frames, version);
    final int trivialLength = trivialSerialized.readableBytes();
    final ExpectedOutput trivialExpected = output(frames(
        Lists.newArrayList(concat(envelope, content))));

    // Test parsing fragmented input
    final VerifyingDecoder verifier = new VerifyingDecoder();
    final ZMTPMessageParser<Void> parser = ZMTPMessageParser.create(version, limit.value, verifier);
    new Fragmenter(serialized.readableBytes()).fragment(new Fragmenter.Consumer() {
      @Override
      public void fragments(final int[] limits, final int count) throws Exception {
        verifier.expect(expected);
        serialized.setIndex(0, serializedLength);
        for (int i = 0; i < count; i++) {
          final int limit = limits[i];
          serialized.writerIndex(limit);
          parser.parse(serialized);
        }
        verifier.assertFinished();

        // Verify that the parser can be reused to parse the same message
        serialized.setIndex(0, serializedLength);
        parser.parse(serialized);
        verifier.assertFinished();

        // Verify that the parser can be reused to parse a well-behaved message
        verifier.expect(trivialExpected);
        trivialSerialized.setIndex(0, trivialLength);
        parser.parse(trivialSerialized);
        verifier.assertFinished();
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

    public static Parameters test(final List<String> input, final ExpectedOutput expected) {
      return test(input, expectation(expected));
    }

    public static Parameters test(final List<String> input, final Expectation... expectations) {
      final List<Verification> verifications = Lists.newArrayList();
      for (final Expectation expectation : expectations) {
        verifications.add(new Verification(input, expectation));
      }
      return new Parameters(input, verifications);
    }

    public static List<String> input(final String... frames) {
      return asList(frames);
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

    public static ExpectedOutput output(final String... frames) {
      return new ExpectedOutput(frames(frames));
    }

    public static ExpectedOutput output(final List<ByteBuf> frames) {
      return new ExpectedOutput(frames);
    }

    public static ExpectedOutput output(final List<ByteBuf> frames, final List<Integer> discarded) {
      return new ExpectedOutput(frames, discarded);
    }

    public static Expectation expectation(final ExpectedOutput expected) {
      return new Expectation(unlimited(), expected);
    }

    public static Expectation expectation(final Limit sizeLimit,
                                          final ExpectedOutput expected) {
      return new Expectation(sizeLimit, expected);
    }

    public List<Verification> verifications() {
      return verifications;
    }
  }

  public static ByteBuf serialize(final boolean enveloped, final ZMTPMessage message, int version) {
    final ByteBuf buffer = Unpooled.buffer(ZMTPUtils.messageSize(
        message, enveloped, version));
    ZMTPUtils.writeMessage(message, buffer, enveloped, version);
    return buffer;
  }

  private ByteBuf serialize(final List<String> frames, int version) {
    return serialize(false, ZMTPMessage.fromStringsUTF8(false, frames), version);
  }

  static class Verification {

    final List<String> input;
    final Expectation expectation;

    public Verification(final List<String> input,
                        final Expectation expectation) {
      this.input = input;
      this.expectation = expectation;
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

  static class Expectation {

    final Limit sizeLimit;
    final ExpectedOutput output;

    Expectation(final Limit sizeLimit) {
      this.sizeLimit = sizeLimit;
      this.output = null;
    }

    Expectation(final Limit sizeLimit, final ExpectedOutput output) {
      this.sizeLimit = sizeLimit;
      this.output = output;
    }
  }
}
