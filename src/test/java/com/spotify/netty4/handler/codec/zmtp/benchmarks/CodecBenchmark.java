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

import com.google.common.collect.Lists;

import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessage;
import com.spotify.netty4.handler.codec.zmtp.ZMTPDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPParser;
import com.spotify.netty4.handler.codec.zmtp.ZMTPParsingException;
import com.spotify.netty4.handler.codec.zmtp.ZMTPUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP20;

@State(Scope.Benchmark)
public class CodecBenchmark {

  private final List<Object> out = Lists.newArrayList();

  private final ZMTPMessage message = ZMTPMessage.fromUTF8(
      "first identity frame",
      "second identity frame",
      "",
      "datadatadatadatadatadatadatadatadatadata",
      "datadatadatadatadatadatadatadatadatadata",
      "datadatadatadatadatadatadatadatadatadata",
      "datadatadatadatadatadatadatadatadatadata");

  private final ZMTPParser messageParserZMTP10 =
      ZMTPParser.create(ZMTP10, new ZMTPMessageDecoder(Integer.MAX_VALUE));

  private final ZMTPParser messageParserZMTP20 =
      ZMTPParser.create(ZMTP20, new ZMTPMessageDecoder(Integer.MAX_VALUE));

  private final ZMTPParser discardingParserZMTP10 =
      ZMTPParser.create(ZMTP10, new Discarder());

  private final ZMTPParser discardingParserZMTP20 =
      ZMTPParser.create(ZMTP20, new Discarder());

  private final ByteBuf incomingZMTP10;
  private final ByteBuf incomingZMTP20;

  private final ByteBuf tmp = PooledByteBufAllocator.DEFAULT.buffer(4096);

  {
    incomingZMTP10 = PooledByteBufAllocator.DEFAULT.buffer(ZMTPUtils.messageSize(message, ZMTP10));
    incomingZMTP20 = PooledByteBufAllocator.DEFAULT.buffer(ZMTPUtils.messageSize(message, ZMTP20));
    ZMTPUtils.writeMessage(message, incomingZMTP10, ZMTP10);
    ZMTPUtils.writeMessage(message, incomingZMTP20, ZMTP20);
  }

  @SuppressWarnings("ForLoopReplaceableByForEach")
  private void consumeAndRelease(final Blackhole bh, final List<Object> out) {
    for (int i = 0; i < out.size(); i++) {
      final Object o = out.get(i);
      bh.consume(o);
      ReferenceCountUtil.release(o);
    }
    out.clear();
  }

  @Benchmark
  public void parsingToMessageZMTP10(final Blackhole bh) throws ZMTPParsingException {
    messageParserZMTP10.parse(incomingZMTP10.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public void parsingToMessageZMTP20(final Blackhole bh) throws ZMTPParsingException {
    messageParserZMTP20.parse(incomingZMTP20.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public void discardingZMTP10(final Blackhole bh) throws ZMTPParsingException {
    discardingParserZMTP10.parse(incomingZMTP10.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public void discardingZMTP20(final Blackhole bh) throws ZMTPParsingException {
    discardingParserZMTP20.parse(incomingZMTP20.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public Object encodingZMTP10() {
    ZMTPUtils.writeMessage(message, tmp.setIndex(0, 0), ZMTP10);
    return tmp;
  }

  @Benchmark
  public Object encodingZMTP20() {
    ZMTPUtils.writeMessage(message, tmp.setIndex(0, 0), ZMTP20);
    return tmp;
  }

  public static void main(final String... args) throws RunnerException, InterruptedException {
    Options opt = new OptionsBuilder()
        .include(".*")
        .forks(1)
        .build();

    new Runner(opt).run();
  }


  private class Discarder implements ZMTPDecoder {

    private int size;


    @Override
    public void header(final long length, final boolean more, final List<Object> out) {
      this.size += size;
    }

    @Override
    public void content(final ByteBuf data, final List<Object> out) {
      data.skipBytes(data.readableBytes());
    }

    @Override
    public void finish(final List<Object> out) {
    }
  }
}
