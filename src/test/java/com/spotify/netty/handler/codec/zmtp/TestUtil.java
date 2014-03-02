package com.spotify.netty.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;

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
    ByteBuf cb = Unpooled.buffer(bytes.length);
    cb.writeBytes(bytes(bytes));
    return cb;
  }

  public static void cmp(ByteBuf buf, int... bytes) {
    cmp(buf, buf(bytes));
  }

  /**
   * Compare Unpooled left and right and raise an Assert.fail() if there are differences
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
          printBytes(expected.array(), expectedPos, expectedReadableCount),
          printBytes(actual.array(), actualPos, actualReadableCount)));
    }
    final int readableBytes = expected.readableBytes();
    for (int i = 0; i < readableBytes; i++) {
      byte lb = expected.readByte();
      byte rb = actual.readByte();
      if (lb != rb) {
        Assert.fail(String.format("Pos %d: (%s != %s)", i,
            printBytes(expected.array(), expectedPos, expectedReadableCount),
            printBytes(actual.array(), actualPos, actualReadableCount)));
      }
    }
  }

  /**
   * Returns a clone of a ByteBuf.
   * @param buf the ByteBuf to clone
   * @return a clone.
   */
  public static ByteBuf clone(ByteBuf buf) {
    return Unpooled.wrappedBuffer(buf.array());
  }

  /*
   * Useful for debugging stuff.

  public static String print(ByteBuf buf) {
    return printBytes(buf.array(), buf.readerIndex(), buf.readableBytes());
  }
  */

  public static String printBytes(byte[] buffer, int start, int length) {
    StringBuilder sb = new StringBuilder(length - start);
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("%%%02x", buffer[i]));
    }
    return sb.toString();
  }

}
