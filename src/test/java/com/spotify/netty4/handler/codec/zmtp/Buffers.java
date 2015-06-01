package com.spotify.netty4.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

class Buffers {

  public static byte[] bytes(int... bytes) {
    byte[] bs = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      bs[i] = (byte) bytes[i];
    }
    return bs;
  }

  public static ByteBuf buf(int... bytes) {
    return buf(bytes(bytes));
  }

  public static ByteBuf buf(byte[] data) {
    return Unpooled.copiedBuffer(data);
  }
}
