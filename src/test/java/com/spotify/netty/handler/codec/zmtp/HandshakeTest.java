package com.spotify.netty.handler.codec.zmtp;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.buf;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests the handshake protocol
 */
@RunWith(MockitoJUnitRunner.class)
public class HandshakeTest {
  byte[] FOO = "foo".getBytes();
  byte[] BAR = "bar".getBytes();

  @Mock Channel channel;

  @Before
  public void setup() {
  }

  @Test
  public void testOnConnect() {
    ZMTPHandshaker h = new ZMTP10Handshaker(new ZMTPSession(ZMTPConnectionType.ADDRESSED, FOO));
    cmp(h.onConnect(), 0x04, 0x00, 0x66, 0x6f, 0x6f);

    h = new ZMTP10Handshaker(new ZMTPSession(ZMTPConnectionType.ADDRESSED, new byte[0]));
    cmp(h.onConnect(), 0x01, 0x00);

    h = new ZMTP20Handshaker(
        socketType, true, localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f);

    h = new ZMTP20Handshaker(
        socketType, false, localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x03, 0x00, 3, 0x66, 0x6f, 0x6f);
  }

  @Test
  public void test1to1Handshake() throws Exception {
    ZMTP10Handshaker h = new ZMTP10Handshaker(new ZMTPSession(ZMTPConnectionType.ADDRESSED, FOO));
    cmp(h.onConnect(), 0x04, 0x00, 0x66, 0x6f, 0x6f);
    ZMTPHandshake handshake = h.inputOutput(buf(0x04, 0x00, 0x62, 0x61, 0x72), channel);
    assertNotNull(handshake);
    verifyZeroInteractions(channel);
    assertEquals(ZMTPHandshake.of(1, BAR), handshake);
  }

  @Test
  public void test2InteropTo1Handshake() throws Exception {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, true,
        localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ZMTPHandshake handshake = h.inputOutput(buf(0x04, 0x00, 0x62, 0x61, 0x72), channel);
    assertNotNull(handshake);
    verify(channel).writeAndFlush(buf(0x66, 0x6f, 0x6f));
    assertEquals(ZMTPHandshake.of(1, BAR), handshake);
  }

  @Test
  public void test2InteropTo2InteropHandshake() throws Exception {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, true,
        localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ZMTPHandshake handshake;
    handshake = h.inputOutput(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f), channel);
    assertNull(handshake);
    verify(channel).writeAndFlush(buf(0x01, 0x02, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    handshake = h.inputOutput(buf(0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72), channel);
    assertNotNull(handshake);
    verifyNoMoreInteractions(channel);
    assertEquals(ZMTPHandshake.of(2, BAR), handshake);
  }

  @Test
  public void test2InteropTo2Handshake() throws Exception {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, true,
        localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ByteBuf cb = buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72);
    ZMTPHandshake handshake;
    handshake = h.inputOutput(cb, channel);
    assertNull(handshake);
    verify(channel).writeAndFlush(buf(0x01, 0x02, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    handshake = h.inputOutput(cb, channel);
    assertNotNull(handshake);
    verifyNoMoreInteractions(channel);
    assertEquals(ZMTPHandshake.of(2, BAR), handshake);
  }

  @Test
  public void test2To2InteropHandshake() throws Exception {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, false,
        localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);

    try {
      h.inputOutput(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f), channel);
      fail("not enough data in greeting (because compat mode) shuld have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    ZMTPHandshake handshake = h.inputOutput(
        buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), channel);
    assertNotNull(handshake);
    assertEquals(ZMTPHandshake.of(2, BAR), handshake);
  }

  @Test
  public void test2To2Handshake() throws Exception {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, false, localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);
    ZMTPHandshake handshake = h.inputOutput(buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), channel);
    assertNotNull(handshake);
    assertEquals(ZMTPHandshake.of(2, BAR), handshake);
  }

  @Test
  public void test2To1Handshake() {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, false,
        localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);
    try {
      assertNull(h.inputOutput(buf(0x04, 0, 0x62, 0x61, 0x72), channel));
      fail("An ZMTP/1 greeting is invalid in plain ZMTP/2. Should have thrown exception");
    } catch (ZMTPException e) {
      // pass
    }
  }

  @Test
  public void test2To2CompatTruncated() throws Exception {
    ZMTP20Handshaker h = new ZMTP20Handshaker(
        socketType, true, localIdentity);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 9, 0x7f);
    ZMTPHandshake handshake = h.inputOutput(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0x7f, 1, 5), channel);
    assertNull(handshake);
    verify(channel).writeAndFlush(buf(1, 2, 0, 8, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79));
  }

  @Test
  public void testParseZMTP2Greeting() throws Exception {
    ByteBuf b = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0x00, 0x01, 0x61);
    assertArrayEquals("a".getBytes(), ZMTP20Handshaker.parseZMTP2Greeting(b, true));
  }

  @Test
  public void testReadZMTP1RemoteIdentity() throws Exception {
    byte[] bs = ZMTPUtils.readZMTP1RemoteIdentity(buf(0x04, 0x00, 0x62, 0x61, 0x72));
    assertArrayEquals(BAR, bs);

    // anonymous handshake
    bs = ZMTPUtils.readZMTP1RemoteIdentity(buf(0x01, 0x00));
    assertNull(bs);
  }

  @Test
  public void testTypeToConst() {
    assertEquals(8, ZMTPSocketType.PUSH.ordinal());
  }

  @Test
  public void testDetectProtocolVersion() {
    try {
      ZMTP20Handshaker.detectProtocolVersion(Unpooled.wrappedBuffer(new byte[0]));
      fail("Should have thown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    try {
      ZMTP20Handshaker.detectProtocolVersion(buf(0xff, 0, 0, 0));
      fail("Should have thown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }

    assertEquals(1, ZMTP20Handshaker.detectProtocolVersion(buf(0x07)));
    assertEquals(1, ZMTP20Handshaker.detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0)));

    assertEquals(2, ZMTP20Handshaker.detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 1)));

  }

  @Test
  public void testParseZMTP2GreetingMalformed() {
    try {
      ByteBuf b = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0xf0, 0x01, 0x61);
      ZMTP20Handshaker.parseZMTP2Greeting(b, true);
      fail("13th byte is not 0x00, should throw exception");
    } catch (ZMTPException e) {
      // pass
    }
  }

}
