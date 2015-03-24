package com.spotify.netty4.handler.codec.zmtp;

import org.junit.Assert;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Reused static methods.
 */
class TestUtil {

  public static byte[] bytes(int ...bytes) {
    byte[] bs = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      bs[i] = (byte)bytes[i];
    }
    return bs;
  }

  public static ByteBuf buf(int ...bytes) {
    return buf(bytes(bytes));
  }

  public static ByteBuf buf(byte[] data) {
    return Unpooled.copiedBuffer(data);
  }

  public static void cmp(ByteBuf buf, int... bytes) {
    cmp(buf, buf(bytes));
  }

  /**
   * Compare ChannelBuffers left and right and raise an Assert.fail() if there are differences
   *
   * @param expected the ByteBuf you expect
   * @param actual the ByteBuf you actually got
   */
  public static void cmp(ByteBuf expected, ByteBuf actual) {
    int expectedPos = expected.readerIndex();
    int actualPos = actual.readerIndex();
    int expectedReadableCount = expected.readableBytes();
    int actualReadableCount = actual.readableBytes();
    if (expectedReadableCount != actualReadableCount) {
      Assert.fail(String.format("Expected same number of readable bytes in buffers (%s != %s)",
          printBytes(expected, expectedPos, expectedReadableCount),
          printBytes(actual, actualPos, actualReadableCount)));
    }
    final int readableBytes = expected.readableBytes();
    for (int i = 0; i < readableBytes; i++) {
      byte lb = expected.readByte();
      byte rb = actual.readByte();
      if (lb != rb) {
        Assert.fail(String.format("Pos %d: (%s != %s)", i,
            printBytes(expected, expectedPos, expectedReadableCount),
            printBytes(actual, actualPos, actualReadableCount)));
      }
    }
  }

  /**
   * Returns a clone of a ByteBuf.
   * @param buf the ByteBuf to clone
   * @return a clone.
   */
  public static ByteBuf clone(ByteBuf buf) {
    return Unpooled.copiedBuffer(buf);
  }

  /*
   * Useful for debugging stuff.

  public static String print(ByteBuf buf) {
    return printBytes(buf.array(), buf.readerIndex(), buf.readableBytes());
  }
  */

  public static String printBytes(byte[] buffer, int start, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("%%%02x", buffer[i]));
    }
    return sb.toString();
  }

  public static String printBytes(ByteBuf buffer, int start, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("%%%02x", buffer.getByte(i)));
    }
    return sb.toString();
  }

}
