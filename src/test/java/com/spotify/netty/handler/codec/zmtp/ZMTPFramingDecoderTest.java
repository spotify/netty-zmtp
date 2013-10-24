package com.spotify.netty.handler.codec.zmtp;


import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Tests ZMTPFramingDecoder
 */
public class ZMTPFramingDecoderTest {
  @Mock Channel channel;
  @Mock ChannelStateEvent channelStateEvent;
  @Mock ChannelHandlerContext ctx;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(channelStateEvent.getChannel()).thenReturn(channel);
    when(ctx.getChannel()).thenReturn(channel);
    when(channel.write(anyObject())).thenReturn(Channels.succeededFuture(channel));
  }

  @Test
  public void testHandshakeWithIdentity() throws Exception {
    doHandshake("something".getBytes(), "another_thing".getBytes());
  }

  @Test
  public void testHandshakeWithoutIdentity() throws Exception {
    doHandshake(null, null);
  }

  @Test
  public void testDecodeFrame() throws Exception {
    byte[] serverIdentity = "server".getBytes();
    byte[] clientIdentity = "client".getBytes();
    ZMTPFramingDecoder zfd = doHandshake(serverIdentity, clientIdentity);
    // for now, broadcast and neutral type seems to be buggy. Going with addressed
    ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
    // header
    byte[] header = "head".getBytes();
    cb.writeByte(header.length + 1);
    cb.writeByte(0x01);
    cb.writeBytes(header);
    // delimiter
    cb.writeByte(0x01);
    cb.writeByte(0x01);
    // body
    byte[] body = "body".getBytes();
    cb.writeByte(body.length + 1);
    cb.writeByte(0x00);
    cb.writeBytes(body);

    ZMTPIncomingMessage zim = (ZMTPIncomingMessage)zfd.decode(ctx, channel, cb);
    ZMTPSession s = zim.getSession();
    Assert.assertArrayEquals(clientIdentity, s.getRemoteIdentity());
    Assert.assertArrayEquals(serverIdentity, s.getLocalIdentity());
    List<ZMTPFrame> frames = zim.getMessage().getContent();

    Assert.assertEquals(1, frames.size());
    Assert.assertArrayEquals("body".getBytes(), frames.get(0).getData());
  }

  @Test
  public void testTruncatedClientIdentity() throws Exception {
    byte[] serverIdentity = "third_thing".getBytes();

    ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdentity);
    ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

    // Someone connects
    zfd.channelConnected(ctx, channelStateEvent);
    verify(channel, times(1)).write(makeFrame(serverIdentity));
    verify(ctx, never()).sendUpstream(channelStateEvent);

    Assert.assertNull(zfd.decode(ctx, channel, ChannelBuffers.dynamicBuffer(0)));

    ChannelBuffer half_identity = ChannelBuffers.dynamicBuffer();
    half_identity.writeByte(7);
    half_identity.writeByte(0);
    half_identity.writeBytes("foo".getBytes());

    Assert.assertNull(zfd.decode(ctx, channel, half_identity));
  }

  @Test
  public void testMalformedClientIdentity() throws Exception {
    byte[] serverIdentity = "third_thing".getBytes();

    ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdentity);
    ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

    // Someone connects
    zfd.channelConnected(ctx, channelStateEvent);
    verify(channel, times(1)).write(makeFrame(serverIdentity));
    verify(ctx, never()).sendUpstream(channelStateEvent);

    ChannelBuffer malformed = ChannelBuffers.dynamicBuffer();
    malformed.writeByte(7);
    malformed.writeByte(0x01);
    malformed.writeBytes("foobar".getBytes());

    Assert.assertNull(zfd.decode(ctx, channel, malformed));

  }

  /**
   * Lets test the funky special case where a shorter length than 255 is encoded in a
   * big endian long, which MAY be done according to spec.
   */
  @Test
  public void testOverlyLongLength() throws Exception {
    byte[] serverIdentity = "fourth".getBytes();
    byte[] clientIdentity = "fifth".getBytes();

    ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdentity);
    ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

    // Someone connects
    zfd.channelConnected(ctx, channelStateEvent);
    verify(channel, times(1)).write(makeFrame(serverIdentity));
    verify(ctx, never()).sendUpstream(channelStateEvent);

    ChannelBuffer funky = ChannelBuffers.dynamicBuffer();
    funky.writeBytes(new byte[]{(byte) 0xff, (byte)0, (byte)0, (byte)0, (byte)0,
        (byte)0, (byte)0, (byte)0, (byte)(clientIdentity.length + 1), (byte) 0});
    funky.writeBytes(clientIdentity);

    Assert.assertNull(zfd.decode(ctx, channel, funky));
    Assert.assertArrayEquals(clientIdentity, s.getRemoteIdentity());
  }


  /**
   * The ZMTP/1.0 spec states that "An identity greeting consists of a unique string of 1
   * to 255 octets". Let's make sure we don't accept longer identities.
   * @throws Exception
   */
  @Test
  public void testOverlyLongIdentity() throws Exception {
    byte[] overlyLong = new byte[256];
    Arrays.fill(overlyLong, (byte)'a');
    try {
      doHandshake("server".getBytes(), overlyLong);
      Assert.fail("Should have thrown exception");
    } catch (ZMTPException e) {
      //pass
    }
  }

  private ZMTPFramingDecoder doHandshake(byte[] serverIdent, byte[] clientIdent) throws Exception
  {
    ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdent);
    ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

    // Someone connects
    zfd.channelConnected(ctx, channelStateEvent);

    verify(channel, times(1)).write(makeFrame(serverIdent));

    verify(ctx, never()).sendUpstream(channelStateEvent);

    // send it
    Assert.assertNull(zfd.decode(ctx, channel, makeFrame(clientIdent)));

    verify(ctx, times(1)).sendUpstream(channelStateEvent);
    return zfd;
  }

  private ChannelBuffer makeFrame(byte[] identity) {
    if (identity == null) {
      identity = new byte[0];
    }
    ChannelBuffer cb = ChannelBuffers.dynamicBuffer(identity.length + 2);
    long l = identity.length + 1;
    if (l < 253) {
      cb.writeByte((byte)l);
    } else {
      cb.writeByte(0xff);
      if (cb.order() == ByteOrder.BIG_ENDIAN) {
        cb.writeLong(l);
      }else {
        cb.writeLong(ChannelBuffers.swapLong(l));
      }
    }

    cb.writeByte(0x00);
    cb.writeBytes(identity);
    return cb;
  }

}
