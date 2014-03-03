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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.*;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Limit.limit;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Limit.unlimited;
import static com.spotify.netty.handler.codec.zmtp.ZMTPMessageParserTest.Parameters.*;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.out;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

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

    @DataPoints
    public static Parameters[] PARAMETERS = {
            test(input("1"), enveloped()),
            test(input("2", ""), enveloped()),
            test(input("3", "aa"), enveloped()),
            test(input("4", "", "a"), enveloped()),
            test(input("5", "", "a", "bb"), enveloped()),
            test(input("6", "aa", "", "b", "cc"), enveloped()),

            test(input("7", "", "a"), nonEnveloped()),
            test(input("8", "", "b", "cc"), nonEnveloped()),
            test(input("9", "aa", "", "b", "cc"), nonEnveloped()),

            test(input("a", "bb", "", "c", "dd", "", "eee"),
                    // Test non-enveloped parsing
                    nonEnveloped(limit(1), truncated(9, "a")),
                    nonEnveloped(limit(2), truncated(9, "a")),
                    nonEnveloped(limit(3), truncated(9, "a", "bb", "")),
                    nonEnveloped(limit(4), truncated(9, "a", "bb", "", "c")),
                    nonEnveloped(limit(5), truncated(9, "a", "bb", "", "c")),
                    nonEnveloped(limit(6), truncated(9, "a", "bb", "", "c", "dd", "")),
                    nonEnveloped(limit(7), truncated(9, "a", "bb", "", "c", "dd", "")),
                    nonEnveloped(limit(8), truncated(9, "a", "bb", "", "c", "dd", "")),
                    nonEnveloped(limit(9), whole(9, "a", "bb", "", "c", "dd", "", "eee")),
                    // Test enveloped parsing
                    enveloped(limit(1), truncated(9, "a")),
                    enveloped(limit(2), truncated(9, "a")),
                    enveloped(limit(3), truncated(9, "a", "bb", "")),
                    enveloped(limit(4), truncated(9, "a", "bb", "", "c")),
                    enveloped(limit(5), truncated(9, "a", "bb", "", "c")),
                    enveloped(limit(6), truncated(9, "a", "bb", "", "c", "dd", "")),
                    enveloped(limit(7), truncated(9, "a", "bb", "", "c", "dd", "")),
                    enveloped(limit(8), truncated(9, "a", "bb", "", "c", "dd", "")),
                    enveloped(limit(9), whole(9, "a", "bb", "", "c", "dd", "", "eee"))
            ),

    };

    @Theory
    public void testParse(final Parameters parameters) throws Exception {
        final List<Future<?>> futures = newArrayList();
        for (final Verification v : parameters.getVerifications()) {
            futures.add(EXECUTOR.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    testParse(v.enveloped, v.sizeLimit, v.inputFrames, v.expectedMessage, 1);
                    testParse(v.enveloped, v.sizeLimit, v.inputFrames, v.expectedMessage, 2);
                    return null;
                }
            }));
        }
        for (final Future<?> future : futures) {
            Uninterruptibles.getUninterruptibly(future);
        }
    }

    private void testParse(final boolean enveloped, final Limit limit, final List<String> input,
                           final ZMTPParsedMessage expected, int version) throws Exception {
        out.println(format("enveloped=%s limit=%s input=%s expected=%s",
                enveloped, limit, input, expected));

        final ByteBuf serialized = serialize(input, version);
        final int serializedLength = serialized.readableBytes();

        // Test parsing the whole message
        {
            final ZMTPMessageParser parser = new ZMTPMessageParser(enveloped, limit.value, 1);
            final ZMTPParsedMessage parsed = parser.parse(serialized);
            serialized.setIndex(0, serializedLength);
            assertEquals(expected, parsed);
        }

        // Prepare for trivial message parsing test
        final int contentSize = min(limit.value - 1, 10);
        final List<String> envelope = asList("e", "");
        final List<String> content = nCopies(contentSize, ".");
        final List<String> frames = newArrayList(concat(envelope, content));
        final ZMTPMessage trivialMessage = ZMTPMessage.fromStringsUTF8(enveloped, frames);
        final ByteBuf trivialSerialized = serialize(frames, version);
        final int trivialLength = trivialSerialized.readableBytes();

        // Test parsing fragmented input
        new Fragmenter(serialized.readableBytes()).fragment(new Fragmenter.Consumer() {
            @Override
            public void fragments(final int[] limits, final int count) throws Exception {
                serialized.setIndex(0, serializedLength);
                ZMTPParsedMessage parsed = null;
                final ZMTPMessageParser parser = new ZMTPMessageParser(enveloped, limit.value, 1);
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
                final ZMTPMessage message = ZMTPMessage.fromStringsUTF8(e.enveloped, input);
                expected = new ZMTPParsedMessage(false, byteSize(input), message);
            } else {
                expected = e.expected;
            }
            return new Verification(e.enveloped, e.sizeLimit, input, expected);
        }

        private static long byteSize(final List<String> frames) {
            long size = 0;
            for (final String frame : frames) {
                size += frame.getBytes().length;
            }
            return size;
        }

        public static Expectation nonEnveloped() {
            return nonEnveloped(unlimited());
        }

        public static Expectation nonEnveloped(final Limit sizeLimit) {
            return expectation(false, sizeLimit);
        }

        public static Expectation nonEnveloped(final Limit sizeLimit,
                                               final ZMTPParsedMessage expected) {
            return expectation(false, sizeLimit, expected);
        }

        public static Expectation nonEnveloped(final Limit sizeLimit, final Output o) {
            final ZMTPMessage message = ZMTPMessage.fromStringsUTF8(false, o.frames);
            final ZMTPParsedMessage expected = new ZMTPParsedMessage(o.truncated, o.byteSize, message);
            return expectation(false, sizeLimit, expected);
        }

        public static Expectation enveloped() {
            return expectation(true, unlimited());
        }

        public static Expectation enveloped(final Limit sizeLimit) {
            return expectation(true, sizeLimit);
        }

        public static Expectation enveloped(final Limit sizeLimit, final Output o) {
            final ZMTPMessage message = ZMTPMessage.fromStringsUTF8(true, o.frames);
            final ZMTPParsedMessage expected = new ZMTPParsedMessage(o.truncated, o.byteSize, message);
            return expectation(true, sizeLimit, expected);
        }

        public static Expectation enveloped(final Limit sizeLimit, final ZMTPParsedMessage expected) {
            return expectation(true, sizeLimit, expected);
        }

        public static List<String> input(final String... frames) {
            return asList(frames);
        }

        public static List<String> frames(final String... frames) {
            return asList(frames);
        }

        public static Output whole(final long byteSize, final String... frames) {
            return output(false, byteSize, frames);
        }

        public static Output truncated(final long byteSize, final String... frames) {
            return output(true, byteSize, frames);
        }

        public static Output output(final boolean truncated, final long byteSize,
                                    final String... frames) {
            return new Output(truncated, byteSize, asList(frames));
        }

        public static ZMTPMessage envelopedMessage(final String... frames) {
            return message(true, frames);
        }

        public static ZMTPMessage nonEnvelopedMessage(final String... frames) {
            return message(false, frames);
        }

        static ZMTPMessage message(final boolean enveloped, final String... frames) {
            return ZMTPMessage.fromStringsUTF8(enveloped, frames);
        }

        public static Expectation expectation(final boolean enveloped, final Limit sizeLimit) {
            return new Expectation(enveloped, sizeLimit);
        }

        public static Expectation expectation(final boolean enveloped, final Limit sizeLimit,
                                              final ZMTPParsedMessage expected) {
            return new Expectation(enveloped, sizeLimit, expected);
        }

        public List<Verification> getVerifications() {
            return verifications;
        }
    }

    public static ByteBuf serialize(final boolean enveloped, final ZMTPMessage message,
                                          int version) {
        final ByteBuf buffer = Unpooled.buffer(ZMTPUtils.messageSize(
                message, enveloped, version));
        ZMTPUtils.writeMessage(message, buffer, enveloped, 1);
        return buffer;
    }

    private ByteBuf serialize(final List<String> frames, int version) {
        return serialize(false, ZMTPMessage.fromStringsUTF8(false, frames), version);
    }

    static class Verification {

        private final boolean enveloped;
        private final Limit sizeLimit;
        private final List<String> inputFrames;
        private final ZMTPParsedMessage expectedMessage;

        public Verification(final boolean enveloped, final Limit sizeLimit,
                            final List<String> inputFrames, final ZMTPParsedMessage expectedMessage) {

            this.enveloped = enveloped;
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
        private final List<String> frames;

        public Output(final boolean truncated, final long byteSize, final List<String> frames) {
            this.truncated = truncated;
            this.byteSize = byteSize;
            this.frames = frames;
        }
    }

    static class Expectation {

        final boolean enveloped;
        final Limit sizeLimit;
        final ZMTPParsedMessage expected;

        Expectation(final boolean enveloped, final Limit sizeLimit) {
            this.enveloped = enveloped;
            this.sizeLimit = sizeLimit;
            this.expected = null;
        }

        Expectation(final boolean enveloped, final Limit sizeLimit, final ZMTPParsedMessage expected) {
            this.enveloped = enveloped;
            this.sizeLimit = sizeLimit;
            this.expected = expected;
        }
    }
}