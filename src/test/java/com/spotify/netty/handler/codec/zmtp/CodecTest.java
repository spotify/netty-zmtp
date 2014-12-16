package com.spotify.netty.handler.codec.zmtp;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Tests related to CodecBase and ZMTP*Codec
 */
public class CodecTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testOverlyLongIdentity() throws Exception {
    byte[] overlyLong = new byte[256];
    Arrays.fill(overlyLong, (byte) 'a');
    ByteBuf buffer = Unpooled.buffer();
    ZMTPUtils.encodeLength(overlyLong.length + 1, buffer);
    buffer.writeByte(0);
    buffer.writeBytes(overlyLong);

    expectedException.expect(ZMTPException.class);
    ZMTPUtils.readZMTP1RemoteIdentity(buffer);
  }

  @Test
  public void testLongZMTP1FrameLengthMissingLong() {
    ByteBuf buffer = Unpooled.buffer();
    buffer.writeByte(0xFF);
    long size = ZMTPUtils.decodeLength(buffer);
    Assert.assertEquals("Length shouldn't have been determined",
                        -1, size);
  }

  @Test
  public void testLongZMTP1FrameLengthWithLong() {
    ByteBuf buffer = Unpooled.buffer();
    buffer.writeByte(0xFF);
    buffer.writeLong(4);
    long size = ZMTPUtils.decodeLength(buffer);
    Assert.assertEquals("Frame length should be after the first byte",
                        4, size);
  }

  @Test
  public void testZMTP1LenghtEmptyBuffer() {
    ByteBuf buffer = Unpooled.buffer();
    long size = ZMTPUtils.decodeLength(buffer);
    Assert.assertEquals("Empty buffer should return -1 frame length",
                        -1, size);
  }

}
