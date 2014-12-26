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

package com.spotify.netty4.handler.codec.zmtp.benchmarks;

import com.spotify.netty4.handler.codec.zmtp.ZMTPFrame;
import com.spotify.netty4.handler.codec.zmtp.ZMTPIncomingMessage;
import com.spotify.netty4.handler.codec.zmtp.ZMTPIncomingMessageDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessage;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParser;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageParsingException;
import com.spotify.netty4.handler.codec.zmtp.ZMTPUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import static java.util.Arrays.asList;

@State(Scope.Benchmark)
public class CodecBenchmark {

  private final ZMTPMessage message = new ZMTPMessage(
      asList(ZMTPFrame.from("first identity frame"),
             ZMTPFrame.from("second identity frame")),
      asList(ZMTPFrame.from("datadatadatadatadatadatadatadatadatadata"),
             ZMTPFrame.from("datadatadatadatadatadatadatadatadatadata"),
             ZMTPFrame.from("datadatadatadatadatadatadatadatadatadata"),
             ZMTPFrame.from("datadatadatadatadatadatadatadatadatadata")));

  private final ZMTPMessageParser<ZMTPIncomingMessage> messageParserV1 =
      ZMTPMessageParser.create(1, Integer.MAX_VALUE, new ZMTPIncomingMessageDecoder(true));

  private final ZMTPMessageParser<ZMTPIncomingMessage> messageParserV2 =
      ZMTPMessageParser.create(2, Integer.MAX_VALUE, new ZMTPIncomingMessageDecoder(true));

  private final ZMTPMessageParser<?> discardingParserV1 =
      ZMTPMessageParser.create(1, Integer.MAX_VALUE, new Discarder());

  private final ZMTPMessageParser<?> discardingParserV2 =
      ZMTPMessageParser.create(2, Integer.MAX_VALUE, new Discarder());

  private final ByteBuf incomingV1;
  private final ByteBuf incomingV2;

  private final ByteBuf tmp = PooledByteBufAllocator.DEFAULT.buffer(4096);

  {
    incomingV1 = PooledByteBufAllocator.DEFAULT.buffer(ZMTPUtils.messageSize(message, true, 1));
    incomingV2 = PooledByteBufAllocator.DEFAULT.buffer(ZMTPUtils.messageSize(message, true, 2));
    ZMTPUtils.writeMessage(message, incomingV1, true, 1);
    ZMTPUtils.writeMessage(message, incomingV2, true, 2);
  }

  @Benchmark
  public ZMTPIncomingMessage parsingToMessageV1() throws ZMTPMessageParsingException {
    final ZMTPIncomingMessage parsed = messageParserV1.parse(incomingV1.resetReaderIndex());
    parsed.release();
    return parsed;
  }

  @Benchmark
  public ZMTPIncomingMessage parsingToMessageV2() throws ZMTPMessageParsingException {
    final ZMTPIncomingMessage parsed = messageParserV2.parse(incomingV2.resetReaderIndex());
    parsed.release();
    return parsed;
  }

  @Benchmark
  public Object discardingV1() throws ZMTPMessageParsingException {
    return discardingParserV1.parse(incomingV1.resetReaderIndex());
  }

  @Benchmark
  public Object discardingV2() throws ZMTPMessageParsingException {
    return discardingParserV2.parse(incomingV2.resetReaderIndex());
  }

  @Benchmark
  public Object encodingV1() throws ZMTPMessageParsingException {
    ZMTPUtils.writeMessage(message, tmp.setIndex(0, 0), true, 1);
    return tmp;
  }

  @Benchmark
  public Object encodingV2() throws ZMTPMessageParsingException {
    ZMTPUtils.writeMessage(message, tmp.setIndex(0, 0), true, 2);
    return tmp;
  }

  public static void main(final String... args) throws RunnerException, InterruptedException {
    Options opt = new OptionsBuilder()
        .forks(1)
        .build();

    new Runner(opt).run();
  }


  private class Discarder implements ZMTPMessageDecoder<Integer> {

    private int size;

    @Override
    public void readFrame(final ByteBuf data, final int size, final boolean more) {
      data.skipBytes(size);
      this.size += size;
    }

    @Override
    public void discardFrame(final int size, final boolean more) {
      this.size += size;
    }

    @Override
    public Integer finish() {
      return size;
    }
  }
}
