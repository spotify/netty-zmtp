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

import com.google.common.collect.Lists;

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
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPWireFormats.wireFormat;

// FIXME (dano): this benchmark needs to be in this package because it uses some internals

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

  private final ZMTPFramingDecoder messageDecoderZMTP10 =
      new ZMTPFramingDecoder(wireFormat(ZMTP10), new ZMTPMessageDecoder());

  private final ZMTPFramingDecoder messageDecoderZMTP20 =
      new ZMTPFramingDecoder(wireFormat(ZMTP20), new ZMTPMessageDecoder());

  private final ZMTPFramingDecoder discardingDecoderZMTP10 =
      new ZMTPFramingDecoder(wireFormat(ZMTP10), new Discarder());

  private final ZMTPFramingDecoder discardingDecoderZMTP20 =
      new ZMTPFramingDecoder(wireFormat(ZMTP20), new Discarder());

  private final ByteBuf incomingZMTP10;
  private final ByteBuf incomingZMTP20;

  private final ZMTPMessageEncoder encoder = new ZMTPMessageEncoder();
  private final ZMTPWriter writerZMTP10 = ZMTPWriter.create(ZMTP10);
  private final ZMTPWriter writerZMTP20 = ZMTPWriter.create(ZMTP20);

  private final ByteBuf tmp = PooledByteBufAllocator.DEFAULT.buffer(4096);

  {
    incomingZMTP10 = message.write(PooledByteBufAllocator.DEFAULT, ZMTP10);
    incomingZMTP20 = message.write(PooledByteBufAllocator.DEFAULT, ZMTP20);
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
    messageDecoderZMTP10.decode(null, incomingZMTP10.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public void parsingToMessageZMTP20(final Blackhole bh) throws ZMTPParsingException {
    messageDecoderZMTP20.decode(null, incomingZMTP20.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public void discardingZMTP10(final Blackhole bh) throws ZMTPParsingException {
    discardingDecoderZMTP10.decode(null, incomingZMTP10.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public void discardingZMTP20(final Blackhole bh) throws ZMTPParsingException {
    discardingDecoderZMTP20.decode(null, incomingZMTP20.resetReaderIndex(), out);
    consumeAndRelease(bh, out);
  }

  @Benchmark
  public Object encodingZMTP10() {
    writerZMTP10.reset(tmp.setIndex(0, 0));
    encoder.encode(message, writerZMTP10);
    return tmp;
  }

  @Benchmark
  public Object encodingZMTP20() {
    writerZMTP20.reset(tmp.setIndex(0, 0));
    encoder.encode(message, writerZMTP20);
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
    public void header(final ChannelHandlerContext ctx, final long length, final boolean more,
                       final List<Object> out) {
      this.size += size;
    }

    @Override
    public void content(final ChannelHandlerContext ctx, final ByteBuf data,
                        final List<Object> out) {
      data.skipBytes(data.readableBytes());
    }

    @Override
    public void finish(final ChannelHandlerContext ctx, final List<Object> out) {
    }

    @Override
    public void close() {
    }
  }
}
