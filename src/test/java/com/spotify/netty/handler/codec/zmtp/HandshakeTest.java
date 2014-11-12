package com.spotify.netty.handler.codec.zmtp;


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.buf;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests the handshake protocol
 */
public class HandshakeTest {
  byte[] FOO = "foo".getBytes();
  byte[] BAR = "bar".getBytes();

  @Mock HandshakeListener handshakeListener;
  @Mock Channel channel;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testIsDone() {
    ZMTP10Codec h = new ZMTP10Codec(new ZMTPSession(FOO));
    h.setListener(handshakeListener);
    verifyNoMoreInteractions(handshakeListener);
  }

  @Test
  public void testOnConnect() {
    CodecBase h = new ZMTP10Codec(new ZMTPSession(FOO));
    cmp(h.onConnect(), 0x04, 0x00, 0x66, 0x6f, 0x6f);

    h = new ZMTP10Codec(new ZMTPSession(new byte[0]));
    cmp(h.onConnect(), 0x01, 0x00);

    h = new ZMTP20Codec(new ZMTPSession(0, FOO, ZMTPSocketType.SUB),
                        true);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f);

    h = new ZMTP20Codec(new ZMTPSession(0, FOO, ZMTPSocketType.REQ),
                        false);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x03, 0x00, 3, 0x66, 0x6f, 0x6f);
  }

  @Test
  public void test1to1Handshake() throws Exception {
    ZMTP10Codec h = new ZMTP10Codec(new ZMTPSession(FOO));
    h.setListener(handshakeListener);
    cmp(h.onConnect(), 0x04, 0x00, 0x66, 0x6f, 0x6f);
    boolean done = h.inputOutput(buf(0x04, 0x00, 0x62, 0x61, 0x72), channel);
    assertTrue(done);
    verifyZeroInteractions(channel);
    verify(handshakeListener).handshakeDone(1, BAR);
  }

  @Test
  public void test2InteropTo1Handshake() throws Exception {
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(0, FOO, ZMTPSocketType.PUB), true);
    h.setListener(handshakeListener);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    boolean done = h.inputOutput(buf(0x04, 0x00, 0x62, 0x61, 0x72), channel);
    assertTrue(done);
    verify(channel).write(buf(0x66, 0x6f, 0x6f));
    verify(handshakeListener).handshakeDone(1, BAR);
  }

  @Test
  public void test2InteropTo2InteropHandshake() throws Exception {
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(0, FOO, ZMTPSocketType.PUB), true);
    h.setListener(handshakeListener);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    boolean done1 = h.inputOutput(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f), channel);
    assertFalse(done1);
    verify(channel).write(buf(0x01, 0x02, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    boolean done2 = h.inputOutput(buf(0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72), channel);
    assertTrue(done2);
    verifyNoMoreInteractions(channel);
    verify(handshakeListener).handshakeDone(2, BAR);
  }

  @Test
  public void test2InteropTo2Handshake() throws Exception {
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(0, FOO, ZMTPSocketType.PUB), true);
    h.setListener(handshakeListener);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0x04, 0x7f);
    ChannelBuffer cb = buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x01, 0x00, 0x03, 0x62, 0x61, 0x72);
    boolean done1 = h.inputOutput(cb, channel);
    assertFalse(done1);
    verify(channel).write(buf(0x01, 0x02, 0x00, 0x03, 0x66, 0x6f, 0x6f));
    boolean done2 = h.inputOutput(cb, channel);
    assertTrue(done2);
    verifyNoMoreInteractions(channel);
    verify(handshakeListener).handshakeDone(2, BAR);
  }

  @Test
  public void test2To2InteropHandshake() throws Exception {
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(1024, FOO, ZMTPSocketType.PUB), false);
    h.setListener(handshakeListener);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);

    try {
      h.inputOutput(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f), channel);
      fail("not enough data in greeting (because compat mode) shuld have thrown exception");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    boolean done = h.inputOutput(
        buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0x4, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), channel);
    assertTrue(done);
    verify(handshakeListener).handshakeDone(2, BAR);
  }

  @Test
  public void test2To2Handshake() throws Exception {
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(1024, FOO, ZMTPSocketType.PUB),
        false);
    h.setListener(handshakeListener);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x2, 0, 0x3, 0x66, 0x6f, 0x6f);
    boolean done = h.inputOutput(buf(
        0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x1, 0x1, 0, 0x03, 0x62, 0x61, 0x72), channel);
    assertTrue(done);
    verify(handshakeListener).handshakeDone(2, BAR);
  }

  @Test
  public void test2To1Handshake() {
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(1024, FOO, ZMTPSocketType.PUB), false);
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
    ZMTP20Codec h = new ZMTP20Codec(
        new ZMTPSession(1024, "identity".getBytes(), ZMTPSocketType.PUB),
        true);
    cmp(h.onConnect(), 0xff, 0, 0, 0, 0, 0, 0, 0, 9, 0x7f);
    boolean done = h.inputOutput(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0x7f, 1, 5), channel);
    assertFalse(done);
    verify(channel).write(buf(1, 2, 0, 8, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79));
  }

  @Test
  public void testParseZMTP2Greeting() throws Exception {
    ChannelBuffer b = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0x00, 0x01, 0x61);
    assertArrayEquals("a".getBytes(), ZMTP20Codec.parseZMTP2Greeting(b, true));
  }

  @Test
  public void testReadZMTP1RemoteIdentity() throws Exception {
    byte[] bs = ZMTP10Codec.readZMTP1RemoteIdentity(buf(0x04, 0x00, 0x62, 0x61, 0x72));
    assertArrayEquals(BAR, bs);

    // anonymous handshake
    bs = ZMTP10Codec.readZMTP1RemoteIdentity(buf(0x01, 0x00));
    assertNull(bs);
  }

  @Test
  public void testTypeToConst() {
    assertEquals(8, ZMTPSocketType.PUSH.ordinal());
  }

  @Test
  public void testDetectProtocolVersion() {
    try {
      ZMTP20Codec.detectProtocolVersion(ChannelBuffers.wrappedBuffer(new byte[0]));
      fail("Should have thown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }
    try {
      ZMTP20Codec.detectProtocolVersion(buf(0xff, 0, 0, 0));
      fail("Should have thown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) {
      // ignore
    }

    assertEquals(1, ZMTP20Codec.detectProtocolVersion(buf(0x07)));
    assertEquals(1, ZMTP20Codec.detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 0)));

    assertEquals(2, ZMTP20Codec.detectProtocolVersion(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 1, 1)));

  }

  @Test
  public void testParseZMTP2GreetingMalformed() {
    try {
      ChannelBuffer b = buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x01, 0x02, 0xf0, 0x01, 0x61);
      ZMTP20Codec.parseZMTP2Greeting(b, true);
      fail("13th byte is not 0x00, should throw exception");
    } catch (ZMTPException e) {
      // pass
    }
  }

}
