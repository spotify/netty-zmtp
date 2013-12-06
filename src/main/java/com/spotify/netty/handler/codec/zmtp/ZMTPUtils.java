/*
 * Copyright (c) 2012-2013 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.List;
import java.util.UUID;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.jboss.netty.buffer.ChannelBuffers.swapLong;

/**
 * Helper utilities for zmtp protocol
 */
public class ZMTPUtils {

  public static final byte MORE_FLAG = 0x1;
  public static final byte FINAL_FLAG = 0x0;
  public static final ZMTPFrame DELIMITER = ZMTPFrame.create();

  /**
   * Helper to decode a ZeroMQ length field
   *
   * @return length or -1 if not enough bytes available
   */
  static public long decodeLength(final ChannelBuffer in) {
    if (in.readableBytes() < 1) {
      return -1;
    }

    long size = in.readByte() & 0xFF;
    if (size == 0xFF) {
      if (in.readableBytes() < 8) {
        return -1;
      }

      if (in.order() == BIG_ENDIAN) {
        size = in.readLong();
      } else {
        size = swapLong(in.readLong());
      }
    }

    return size;
  }

  /**
   * Helper to encode a zmtp length field
   */
  static public void encodeLength(final long size, final ChannelBuffer out) {
    if (size < 255) {
      // Encoded as a single byte
      out.writeByte((byte) size);
    } else {
      out.writeByte(0xFF);

      if (out.order() == BIG_ENDIAN) {
        out.writeLong(size);
      } else {
        out.writeLong(swapLong(size));
      }
    }
  }

  /**
   * Returns a byte array from a UUID
   *
   * @return byte array format (big endian)
   */
  static public byte[] getBytesFromUUID(final UUID uuid) {
    final long most = uuid.getMostSignificantBits();
    final long least = uuid.getLeastSignificantBits();

    return new byte[]{
        (byte) (most >>> 56), (byte) (most >>> 48), (byte) (most >>> 40),
        (byte) (most >>> 32), (byte) (most >>> 24), (byte) (most >>> 16),
        (byte) (most >>> 8), (byte) most,
        (byte) (least >>> 56), (byte) (least >>> 48), (byte) (least >>> 40),
        (byte) (least >>> 32), (byte) (least >>> 24), (byte) (least >>> 16),
        (byte) (least >>> 8), (byte) least};
  }

  /**
   * Writes a ZMTP frame to a buffer.
   *
   * @param frame  The frame to write.
   * @param buffer The target buffer.
   * @param more   True to write a more flag, false to write a final flag.
   */
  public static void writeFrame(final ZMTPFrame frame, final ChannelBuffer buffer,
                                final boolean more) {
    encodeLength(frame.size() + 1, buffer);
    buffer.writeByte(more ? MORE_FLAG : FINAL_FLAG);
    if (frame.hasData()) {
      buffer.writeBytes(frame.getDataBuffer());
    }
  }

  /**
   * Write a ZMTP message to a buffer.
   *
   * @param message   The message to write.
   * @param buffer    The target buffer.
   * @param enveloped Whether the envelope and delimiter should be written.
   */
  @SuppressWarnings("ForLoopReplaceableByForEach")
  public static void writeMessage(final ZMTPMessage message, final ChannelBuffer buffer,
                                  final boolean enveloped) {

    // Write envelope
    if (enveloped) {
      // Sanity check
      if (message.getContent().isEmpty()) {
        throw new IllegalArgumentException("Cannot write enveloped message with no content");
      }

      final List<ZMTPFrame> envelope = message.getEnvelope();
      for (int i = 0; i < envelope.size(); i++) {
        writeFrame(envelope.get(i), buffer, true);
      }

      // Write the delimiter
      writeFrame(DELIMITER, buffer, true);
    }

    final List<ZMTPFrame> content = message.getContent();
    final int n = content.size();
    final int lastFrame = n - 1;
    for (int i = 0; i < n; i++) {
      writeFrame(content.get(i), buffer, i < lastFrame);
    }
  }

  /**
   * Calculate bytes needed to serialize a ZMTP frame.
   *
   * @param frame The frame.
   * @return Bytes needed.
   */
  public static int frameSize(final ZMTPFrame frame) {
    if (frame.size() + 1 < 255) {
      return 1 + 1 + frame.size();
    } else {
      return 1 + 8 + 1 + frame.size();
    }
  }

  /**
   * Calculate bytes needed to serialize a ZMTP message.
   *
   * @param message   The message.
   * @param enveloped Whether an envelope will be written.
   * @return The number of bytes needed.
   */
  @SuppressWarnings("ForLoopReplaceableByForEach")
  public static int messageSize(final ZMTPMessage message, final boolean enveloped) {
    int size = 0;

    if (enveloped) {

      final List<ZMTPFrame> envelope = message.getEnvelope();
      for (int i = 0; i < envelope.size(); i++) {
        size += frameSize(envelope.get(i));
      }
      size += frameSize(DELIMITER);
    }

    final List<ZMTPFrame> content = message.getContent();
    final int n = content.size();
    for (int i = 0; i < n; i++) {
      size += frameSize(content.get(i));
    }

    return size;
  }

  /**
   * Create a string from binary data, keeping printable ascii and hex encoding everything else.
   *
   * @param data The data
   * @return A string representation of the data
   */
  public static String toString(final byte[] data) {
    if (data == null) {
      return null;
    }
    return toString(ChannelBuffers.wrappedBuffer(data));
  }

  /**
   * Create a string from binary data, keeping printable ascii and hex encoding everything else.
   *
   * @param data The data
   * @return A string representation of the data
   */
  public static String toString(final ChannelBuffer data) {
    if (data == null) {
      return null;
    }
    final StringBuilder sb = new StringBuilder();
    while (data.readable()) {
      final byte b = data.readByte();
      if (b > 31 && b < 127) {
        if (b == '%') {
          sb.append('%');
        }
        sb.append((char) b);
      } else {
        sb.append('%');
        sb.append(String.format("%02X", b));
      }
    }
    return sb.toString();
  }

  public static String toString(final List<ZMTPFrame> frames) {
    final StringBuilder builder = new StringBuilder("[");
    for (int i = 0; i < frames.size(); i++) {
      final ZMTPFrame frame = frames.get(i);
      builder.append('"');
      builder.append(toString(frame.getDataBuffer()));
      builder.append('"');
      if (i < frames.size() - 1) {
        builder.append(',');
      }
    }
    builder.append(']');
    return builder.toString();
  }
}
