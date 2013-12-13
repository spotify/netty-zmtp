package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;

/**
 * Tests FramingEncoder
 */
public class ZMTPFramingEncoderTest {

  private static final byte[] LARGE_FILL = new byte[500];
  static {
    fill(LARGE_FILL, (byte)0x61);
  }

  @Test
  public void testEncodeZMTP1() throws Exception {

    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(1, true);

    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("id0"), ZMTPFrame.create("id1")),
        asList(ZMTPFrame.create("f0")));

    ChannelBuffer buf = (ChannelBuffer)enc.encode(null, null, message);
    cmp(buf, 4, 1, 0x69, 0x64, 0x30, 4, 1, 0x69, 0x64, 0x31, 1, 1, 3, 0, 0x66, 0x30);
  }

  @Test
  public void testEncodeZMTP2() throws Exception {

    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("id0"), ZMTPFrame.create("id1")),
        asList(ZMTPFrame.create("f0")));

    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(2, true);

    ChannelBuffer buf = (ChannelBuffer)enc.encode(null, null, message);
    cmp(buf, 1, 3, 0x69, 0x64, 0x30, 1, 3, 0x69, 0x64, 0x31, 1, 0, 0, 2, 0x66, 0x30);
  }

  @Test
  public void testEncodeZMTP2Long() throws Exception {
    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("id0")),
        asList(ZMTPFrame.create(LARGE_FILL)));
    ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
    buf.writeBytes(bytes(1, 3, 0x69, 0x64, 0x30, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0x01, 0xf4));
    buf.writeBytes(LARGE_FILL);

    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(2, true);

    cmp(buf, (ChannelBuffer)enc.encode(null, null, message));

  }
}
