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

package com.spotify.netty.zmtp;

import com.spotify.netty.handler.codec.zmtp.ZMTPFrame;
import com.spotify.netty.handler.codec.zmtp.ZMTPMessage;
import com.spotify.netty.handler.codec.zmtp.ZMTPMessageParser;
import com.spotify.netty.handler.codec.zmtp.ZMTPMessageParsingException;
import com.spotify.netty.handler.codec.zmtp.ZMTPUtils;

import io.netty.buffer.ByteBuf;

import io.netty.buffer.Unpooled;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.Arrays.asList;

public class Benchmark {

  @Ignore("this is a benchmark")
  @Test
  public void benchmarkEncoding() throws ZMTPMessageParsingException {
    final ProgressMeter meter = new ProgressMeter("messages");
    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("first identity frame"),
               ZMTPFrame.create("second identity frame")),
        asList(ZMTPFrame.create("datadatadatadatadatadatadatadatadatadata"),
               ZMTPFrame.create("datadatadatadatadatadatadatadatadatadata"),
               ZMTPFrame.create("datadatadatadatadatadatadatadatadatadata"),
               ZMTPFrame.create("datadatadatadatadatadatadatadatadatadata")));
    final ZMTPMessageParser parser = new ZMTPMessageParser(true, 1024 * 1024);
    long sum = 0;
    for (long i = 0; i < 1000000; i++) {
      for (long j = 0; j < 1000; j++) {
        final ByteBuf buffer = Unpooled.buffer(ZMTPUtils.messageSize(message, true));
        ZMTPUtils.writeMessage(message, buffer, true);
        message = parser.parse(buffer).getMessage();

        sum += buffer.readableBytes();
        buffer.release();
      }
      meter.inc(1000, 0);
    }
    System.out.println(sum);
  }
}
