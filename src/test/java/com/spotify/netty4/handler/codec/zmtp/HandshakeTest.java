package com.spotify.netty4.handler.codec.zmtp;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import static com.spotify.netty4.handler.codec.zmtp.TestUtil.buf;
import static com.spotify.netty4.handler.codec.zmtp.TestUtil.cmp;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.PUB;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.REQ;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.SUB;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP20;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests the handshake protocol
 */
@RunWith(MockitoJUnitRunner.class)
public class HandshakeTest {

  private static final ByteBuffer FOO = UTF_8.encode("foo");
  private static final ByteBuffer BAR = UTF_8.encode("bar");

  @Mock ChannelHandlerContext ctx;

  @Test
  public void testGreeting() {
    ZMTPHandshaker h = new ZMTP10Protocol.Handshaker(FOO);
    cmp(h.greeting(), 0x04, 0x00, 0x66, 0x6f, 0x6f);

    h = new ZMTP10Protocol.Handshaker(ByteBuffer.allocate(0));
    cmp(h.greeting(), 0x01, 0x00);

    h = new ZMTP20Protocol.Handshaker(SUB, true, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f);

    h = new ZMTP20Protocol.Handshaker(REQ, false, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x03, 0x00, 3, 0x66, 0x6f, 0x6f);
  }

  @Test
  public void test1to1Handshake() throws Exception {
    ZMTP10Protocol.Handshaker h = new ZMTP10Protocol.Handshaker(FOO);
    cmp(h.greeting(), 0x04, 0x00, 0x66, 0x6f, 0x6f);
    ZMTPHandshake handshake = h.handshake(buf(0x04, 0x00, 0x62, 0x61, 0x72), ctx);
    assertNotNull(handshake);
    verifyZeroInteractions(ctx);
    assertEquals(ZMTPHandshake.of(ZMTP10, BAR), handshake);
  }

  @Test
  public void test2InteropTo1Handshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(ROUTER, true, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ZMTPHandshake handshake = h.handshake(buf(0x04, 0x00, 0x62, 0x61, 0x72), ctx);
    assertNotNull(handshake);
    verify(ctx).writeAndFlush(buf(0x66, 0x6f, 0x6f));
    assertEquals(ZMTPHandshake.of(ZMTP10, BAR), handshake);
  }

  @Test
  public void test2InteropTo2InteropHandshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, true, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ZMTPHandshake handshake;
    handshake = h.handshake(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f), ctx);
    assertNull(handshake);
    verify(ctx).writeAndFlush(buf(0x01, 0x02, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    handshake = h.handshake(buf(0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72), ctx);
    assertNotNull(handshake);
    verifyNoMoreInteractions(ctx);
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR), handshake);
  }

  @Test
  public void test2InteropTo2Handshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, true, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ByteBuf cb = buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72);
    ZMTPHandshake handshake;
    handshake = h.handshake(cb, ctx);
    assertNull(handshake);
    verify(ctx).writeAndFlush(buf(0x01, 0x02, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    handshake = h.handshake(cb, ctx);
    assertNotNull(handshake);
    verifyNoMoreInteractions(ctx);
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR), handshake);
  }

  @Test
  public void test2To2InteropHandshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, false, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);

    try {
      h.handshake(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f), ctx);
      fail("not enough data in greeting (because compat mode) shuld have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    ZMTPHandshake handshake = h.handshake(
        buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), ctx);
    assertNotNull(handshake);
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR), handshake);
  }

  @Test
  public void test2To2Handshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, false, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);
    ZMTPHandshake handshake = h.handshake(buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), ctx);
    assertNotNull(handshake);
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR), handshake);
  }


  @Test
  public void test2To1Handshake() {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, false, FOO);
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);
    try {
      assertNull(h.handshake(buf(0x04, 0, 0x62, 0x61, 0x72), ctx));
      fail("An ZMTP/1 greeting is invalid in plain ZMTP/2. Should have thrown exception");
    } catch (ZMTPException e) {
      // pass
    }
  }

  @Test
  public void test2To2CompatTruncated() throws Exception {
    ZMTP20Protocol.Handshaker
        h = new ZMTP20Protocol.Handshaker(PUB, true, UTF_8.encode("identity"));
    cmp(h.greeting(), 0xff, 0, 0, 0, 0, 0, 0, 0, 9, 0x7f);
    ZMTPHandshake handshake = h.handshake(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0x7f, 1, 5), ctx);
    assertNull(handshake);
    verify(ctx).writeAndFlush(buf(1, 2, 0, 8, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79));
  }

  @Test
  public void testParseZMTP2Greeting() throws Exception {
    ByteBuf b = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0x00, 0x01, 0x61);
    assertArrayEquals("a".getBytes(), ZMTP20Protocol.Handshaker.parseZMTP2Greeting(b, true));
  }

  @Test
  public void testReadZMTP1RemoteIdentity() throws Exception {
    byte[] bs = ZMTP10WireFormat.readIdentity(buf(0x04, 0x00, 0x62, 0x61, 0x72));
    assertNotNull(bs);
    assertEquals(BAR, ByteBuffer.wrap(bs));

    // anonymous handshake
    bs = ZMTP10WireFormat.readIdentity(buf(0x01, 0x00));
    assertNotNull(bs);
    assertTrue(bs.length == 0);
  }

  @Test
  public void testTypeToConst() {
    assertEquals(8, ZMTPSocketType.PUSH.ordinal());
  }

  @Test
  public void testDetectProtocolVersion() {
    try {
      ZMTP20Protocol.Handshaker.detectProtocolVersion(Unpooled.wrappedBuffer(new byte[0]));
      fail("Should have thown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    try {
      ZMTP20Protocol.Handshaker.detectProtocolVersion(buf(0xff, 0, 0, 0));
      fail("Should have thown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }

    assertEquals(ZMTP10, ZMTP20Protocol.Handshaker.detectProtocolVersion(buf(0x07)));
    assertEquals(ZMTP10, ZMTP20Protocol.Handshaker
        .detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0)));
    assertEquals(ZMTP20, ZMTP20Protocol.Handshaker
        .detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 1)));
  }

  @Test
  public void testParseZMTP2GreetingMalformed() {
    try {
      ByteBuf b = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0xf0, 0x01, 0x61);
      ZMTP20Protocol.Handshaker.parseZMTP2Greeting(b, true);
      fail("13th byte is not 0x00, should throw exception");
    } catch (ZMTPException e) {
      // pass
    }
  }

}
