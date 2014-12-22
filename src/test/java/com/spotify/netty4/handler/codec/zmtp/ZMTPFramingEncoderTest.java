package com.spotify.netty4.handler.codec.zmtp;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import static com.spotify.netty4.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty4.handler.codec.zmtp.TestUtil.cmp;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZMTPFramingEncoderTest {

  @Mock ChannelHandlerContext ctx;
  @Mock ChannelPromise promise;

  @Captor ArgumentCaptor<ByteBuf> bufCaptor;

  private static final byte[] LARGE_FILL = new byte[500];
  static {
    fill(LARGE_FILL, (byte)0x61);
  }

  @Before
  public void setUp() {
    when(ctx.write(bufCaptor.capture(), eq(promise))).thenReturn(promise);
  }

  @Test
  public void testEncodeZMTP1() throws Exception {

    ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
    session.actualVersion(1);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.from("id0"), ZMTPFrame.from("id1")),
        asList(ZMTPFrame.from("f0")));

    enc.write(ctx, message, promise);
    final ByteBuf buf = bufCaptor.getValue();
    cmp(buf, 4, 1, 0x69, 0x64, 0x30, 4, 1, 0x69, 0x64, 0x31, 1, 1, 3, 0, 0x66, 0x30);
    buf.release();
  }

  @Test
  public void testEncodeZMTP2() throws Exception {

    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.from("id0"), ZMTPFrame.from("id1")),
        asList(ZMTPFrame.from("f0")));

    ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
    session.actualVersion(2);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    enc.write(ctx, message, promise);
    final ByteBuf buf = bufCaptor.getValue();
    cmp(buf, 1, 3, 0x69, 0x64, 0x30, 1, 3, 0x69, 0x64, 0x31, 1, 0, 0, 2, 0x66, 0x30);
    buf.release();
  }

  @Test
  public void testEncodeZMTP2Long() throws Exception {
    ZMTPMessage message = new ZMTPMessage(
        asList(ZMTPFrame.from("id0")),
        asList(ZMTPFrame.from(LARGE_FILL)));
    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(bytes(1, 3, 0x69, 0x64, 0x30,
                         1, 0,
                         2, 0, 0, 0, 0, 0, 0, 0x01, 0xf4));
    buf.writeBytes(LARGE_FILL);

    ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
    session.actualVersion(2);
    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    enc.write(ctx, message, promise);
    final ByteBuf buf2 = bufCaptor.getValue();

    cmp(buf, buf2);

    buf.release();
    buf2.release();
  }
}
