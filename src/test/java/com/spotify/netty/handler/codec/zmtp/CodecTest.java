package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests related to CodecBase and ZMTP*Codec
 */
public class CodecTest {
  @Test
  public void testOverlyLongIdentity() throws Exception {
    byte[] overlyLong = new byte[256];
    Arrays.fill(overlyLong, (byte) 'a');
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    ZMTPUtils.encodeLength(overlyLong.length + 1, buffer);
    buffer.writeByte(0);
    buffer.writeBytes(overlyLong);
    try {
      Assert.assertArrayEquals(overlyLong, CodecBase.readZMTP1RemoteIdentity(buffer));
      Assert.fail("Should have thrown exception");
    } catch (ZMTPException e) {
      //pass
    }
  }

  @Test
  public void testLongZMTP1FrameLengthMissingLong() {
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    buffer.writeByte(0xFF);
    long size = ZMTPUtils.decodeLength(buffer);
    Assert.assertEquals("Length shouldn't have been determined",
                        -1, size);
  }

  @Test
  public void testLongZMTP1FrameLengthWithLong() {
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    buffer.writeByte(0xFF);
    buffer.writeLong(4);
    long size = ZMTPUtils.decodeLength(buffer);
    Assert.assertEquals("Frame length should be after the first byte",
                        4, size);
  }

}
