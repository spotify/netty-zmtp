package com.spotify.netty4.handler.codec.zmtp;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import static com.spotify.netty4.handler.codec.zmtp.Buffers.buf;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.PUB;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.REQ;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.SUB;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPVersion.ZMTP20;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
    assertThat(h.greeting(), is(buf(0x04, 0x00, 0x66, 0x6f, 0x6f)));

    h = new ZMTP10Protocol.Handshaker(ByteBuffer.allocate(0));
    assertThat(h.greeting(), is(buf(0x01, 0x00)));

    h = new ZMTP20Protocol.Handshaker(SUB, FOO, true);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f)));

    h = new ZMTP20Protocol.Handshaker(REQ, FOO, false);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f,
                                    0x01, 0x03, 0x00, 3, 0x66, 0x6f, 0x6f)));
  }

  @Test
  public void test1to1Handshake() throws Exception {
    final ZMTP10Protocol.Handshaker h = new ZMTP10Protocol.Handshaker(FOO);
    assertThat(h.greeting(), is(buf(0x04, 0x00, 0x66, 0x6f, 0x6f)));
    final ZMTPHandshake handshake = h.handshake(buf(0x04, 0x00, 0x62, 0x61, 0x72), ctx);
    assertThat(handshake, is(notNullValue()));
    verifyZeroInteractions(ctx);
    assertEquals(ZMTPHandshake.of(ZMTP10, BAR, null), handshake);
  }

  @Test
  public void test2InteropTo1Handshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(ROUTER, FOO, true);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f)));
    ZMTPHandshake handshake = h.handshake(buf(0x04, 0x00, 0x62, 0x61, 0x72), ctx);
    assertThat(handshake, is(notNullValue()));
    verify(ctx).writeAndFlush(buf(0x66, 0x6f, 0x6f));
    assertEquals(ZMTPHandshake.of(ZMTP10, BAR, null), handshake);
  }

  @Test
  public void test2InteropTo2InteropHandshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, FOO, true);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f)));
    ZMTPHandshake handshake;
    handshake = h.handshake(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f), ctx);
    assertThat(handshake, is(nullValue()));
    verify(ctx).writeAndFlush(buf(0x01, 0x01, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    handshake = h.handshake(buf(0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72), ctx);
    assertThat(handshake, is(notNullValue()));
    verifyNoMoreInteractions(ctx);
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR, PUB), handshake);
  }

  @Test
  public void test2InteropTo2Handshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, FOO, true);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f)));
    ByteBuf cb = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72);
    ZMTPHandshake handshake;
    handshake = h.handshake(cb, ctx);
    assertThat(handshake, is(nullValue()));
    verify(ctx).writeAndFlush(buf(0x01, 0x01, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    handshake = h.handshake(cb, ctx);
    assertThat(handshake, is(notNullValue()));
    verifyNoMoreInteractions(ctx);
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR, PUB), handshake);
  }

  @Test
  public void test2To2InteropHandshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, FOO, false);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x3, 0x66, 0x6f, 0x6f)));

    try {
      h.handshake(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f), ctx);
      fail("not enough data in greeting (because compat mode) should have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    ZMTPHandshake handshake = h.handshake(
        buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), ctx);
    assertThat(handshake, is(notNullValue()));
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR, PUB), handshake);
  }

  @Test
  public void test2To2Handshake() throws Exception {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, FOO, false);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x3, 0x66, 0x6f, 0x6f)));
    ZMTPHandshake handshake = h.handshake(buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), ctx);
    assertThat(handshake, is(notNullValue()));
    assertEquals(ZMTPHandshake.of(ZMTPVersion.ZMTP20, BAR, PUB), handshake);
  }


  @Test
  public void test2To1Handshake() {
    ZMTPHandshaker h = new ZMTP20Protocol.Handshaker(PUB, FOO, false);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x3, 0x66, 0x6f, 0x6f)));
    try {
      assertThat(h.handshake(buf(0x04, 0, 0x62, 0x61, 0x72), ctx), is(nullValue()));
      fail("An ZMTP/1 greeting is invalid in plain ZMTP/2. Should have thrown exception");
    } catch (ZMTPException e) {
      // pass
    }
  }

  @Test
  public void test2To2CompatTruncated() throws Exception {
    final ByteBuffer identity = UTF_8.encode("identity");
    ZMTP20Protocol.Handshaker h = new ZMTP20Protocol.Handshaker(PUB, identity, true);
    assertThat(h.greeting(), is(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 9, 0x7f)));
    ZMTPHandshake handshake = h.handshake(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0x7f, 1, 5), ctx);
    assertThat(handshake, is(nullValue()));
    verify(ctx).writeAndFlush(buf(1, 1, 0, 8, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79));
  }

  @Test
  public void testReadZMTP2Greeting() throws Exception {
    final ByteBuf in = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0x00, 0x01, 0x61);
    final ZMTP20WireFormat.Greeting greeting = ZMTP20WireFormat.readGreeting(in);
    assertThat(greeting.identity(), is(UTF_8.encode("a")));
  }

  @Test
  public void testReadZMTP1RemoteIdentity() throws Exception {
    ByteBuffer identity = ZMTP10WireFormat.readIdentity(buf(0x04, 0x00, 0x62, 0x61, 0x72));
    assertThat(identity, is(notNullValue()));
    assertEquals(BAR, identity);

    // anonymous handshake
    identity = ZMTP10WireFormat.readIdentity(buf(0x01, 0x00));
    assertThat(identity, is(notNullValue()));
    assert identity != null;
    assertThat(identity.remaining(), is(0));
  }

  @Test
  public void testTypeToConst() {
    assertEquals(8, ZMTPSocketType.PUSH.ordinal());
  }

  @Test
  public void testDetectProtocolVersion() {
    try {
      ZMTP20WireFormat.detectProtocolVersion(Unpooled.wrappedBuffer(new byte[0]));
      fail("Should have thrown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    try {
      ZMTP20WireFormat.detectProtocolVersion(buf(0xff, 0, 0, 0));
      fail("Should have thrown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }

    assertEquals(ZMTP10, ZMTP20WireFormat.detectProtocolVersion(buf(0x07)));
    assertEquals(ZMTP10, ZMTP20WireFormat
        .detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0)));
    assertEquals(ZMTP20, ZMTP20WireFormat
        .detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 1)));
  }

  @Test
  public void testReadZMTP2GreetingMalformed() {
    try {
      ByteBuf in = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0xf0, 0x01, 0x61);
      ZMTP20WireFormat.readGreeting(in);
      fail("13th byte is not 0x00, should throw exception");
    } catch (ZMTPException e) {
      // pass
    }
  }
}
