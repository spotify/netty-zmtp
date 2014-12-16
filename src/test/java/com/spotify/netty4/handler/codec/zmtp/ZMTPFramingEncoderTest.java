package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.spotify.netty4.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty4.handler.codec.zmtp.TestUtil.cmp;
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

    ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
    session.actualVersion(1);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("id0"), ZMTPFrame.create("id1")),
        asList(ZMTPFrame.create("f0")));

    final List<Object> out = Lists.newArrayList();
    enc.encode(null, message, out);
    final ByteBuf buf = (ByteBuf) out.get(0);
    cmp(buf, 4, 1, 0x69, 0x64, 0x30, 4, 1, 0x69, 0x64, 0x31, 1, 1, 3, 0, 0x66, 0x30);
  }

  @Test
  public void testEncodeZMTP2() throws Exception {

    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("id0"), ZMTPFrame.create("id1")),
        asList(ZMTPFrame.create("f0")));

    ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
    session.actualVersion(2);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    final List<Object> out = Lists.newArrayList();
    enc.encode(null, message, out);
    final ByteBuf buf = (ByteBuf) out.get(0);
    cmp(buf, 1, 3, 0x69, 0x64, 0x30, 1, 3, 0x69, 0x64, 0x31, 1, 0, 0, 2, 0x66, 0x30);
  }

  @Test
  public void testEncodeZMTP2Long() throws Exception {
    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.create("id0")),
        asList(ZMTPFrame.create(LARGE_FILL)));
    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(bytes(1, 3, 0x69, 0x64, 0x30, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0x01, 0xf4));
    buf.writeBytes(LARGE_FILL);

    ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
    session.actualVersion(2);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    final List<Object> out = Lists.newArrayList();
    enc.encode(null, message, out);
    final ByteBuf buf2 = (ByteBuf) out.get(0);

    cmp(buf, buf2);

  }
}
