package com.spotify.netty.handler.codec.zmtp;

import com.google.common.base.Strings;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;

/**
 * Tests FramingEncoder
 */
public class ZMTPFramingEncoderTest {

  private static final String LARGE_FILL = Strings.repeat(".", 500);

  @Test
  public void testEncodeZMTP1() throws Exception {

    ZMTPSession session = new ZMTPSession(1024);
    session.actualVersion(1);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    ZMTPMessage message = ZMTPMessage.fromStringsUTF8("id0", "id1", "", "f0");

    ChannelBuffer buf = (ChannelBuffer)enc.encode(null, null, message);
    cmp(buf, 4, 1, 0x69, 0x64, 0x30, 4, 1, 0x69, 0x64, 0x31, 1, 1, 3, 0, 0x66, 0x30);
  }

  @Test
  public void testEncodeZMTP2() throws Exception {

    ZMTPMessage message = ZMTPMessage.fromStringsUTF8("id0", "id1", "", "f0");

    ZMTPSession session = new ZMTPSession(1024);
    session.actualVersion(2);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    ChannelBuffer buf = (ChannelBuffer)enc.encode(null, null, message);
    cmp(buf, 1, 3, 0x69, 0x64, 0x30, 1, 3, 0x69, 0x64, 0x31, 1, 0, 0, 2, 0x66, 0x30);
  }

  @Test
  public void testEncodeZMTP2Long() throws Exception {
    ZMTPMessage message = ZMTPMessage.fromStringsUTF8("id0", "", LARGE_FILL);
    ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
    buf.writeBytes(bytes(1, 3, 0x69, 0x64, 0x30, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0x01, 0xf4));
    buf.writeBytes(LARGE_FILL.getBytes(UTF_8));

    ZMTPSession session = new ZMTPSession(1024);
    session.actualVersion(2);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    cmp(buf, (ChannelBuffer)enc.encode(null, null, message));

  }
}
