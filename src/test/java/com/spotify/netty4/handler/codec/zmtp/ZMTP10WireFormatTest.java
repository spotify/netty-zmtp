package com.spotify.netty4.handler.codec.zmtp;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ZMTP10WireFormatTest {

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testTooLongIdentity() throws Exception {
    final ByteBuf buffer = Unpooled.buffer();
    buffer.writeByte(0xFF);
    buffer.writeLong(256 + 1);
    buffer.writeByte(0);
    buffer.writeBytes(new byte[256]);

    expectedException.expect(ZMTPException.class);
    ZMTP10WireFormat.readIdentity(buffer);
  }

  @Test
  public void testLongFrameLengthMissingLong() {
    final ByteBuf buffer = Unpooled.buffer();
    buffer.writeByte(0xFF);
    final long size = ZMTP10WireFormat.readLength(buffer);
    assertThat("Length shouldn't have been determined", size, is(-1L));
  }

  @Test
  public void testLongFrameLengthWithLong() {
    final ByteBuf buffer = Unpooled.buffer();
    buffer.writeByte(0xFF);
    buffer.writeLong(4);
    final long size = ZMTP10WireFormat.readLength(buffer);
    assertThat("Frame length should be after the first byte", size, is(4L));
  }

  @Test
  public void testFrameLengthEmptyBuffer() {
    final ByteBuf buffer = Unpooled.buffer();
    final long size = ZMTP10WireFormat.readLength(buffer);
    assertThat("Empty buffer should return -1 frame length", size, is(-1L));
  }

}
