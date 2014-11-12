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

import java.util.ArrayList;
import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.ZMTPUtils.MORE_FLAG;
import static java.lang.Math.min;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.jboss.netty.buffer.ChannelBuffers.swapLong;

/**
 * Decodes ZMTP messages from a channel buffer, reading and accumulating frame by frame, keeping
 * state and updating buffer reader indices as necessary.
 */
public class ZMTPMessageParser {

  private static final byte LONG_FLAG = 0x02;

  private final long sizeLimit;
  private final int version;

  private List<ZMTPFrame> frames;
  private boolean hasMore;
  private long size;
  private int frameSize;

  // Used by discarding mode
  private int frameRemaining;
  private boolean headerParsed;

  public ZMTPMessageParser(final long sizeLimit, int version) {
    this.sizeLimit = sizeLimit;
    this.version = version;
    reset();
  }

  /**
   * Parses as many whole frames from the buffer as possible, until the final frame is encountered.
   * If the message was completed, it returns the frames of the message. Otherwise it returns null
   * to indicate that more data is needed.
   *
   * <p> Oversized messages will be truncated by discarding frames that would make the message size
   * exceeed the specified size limit.
   *
   * @param buffer Buffer with data
   * @return A {@link ZMTPMessage} if it was completely parsed, otherwise null.
   */
  public ZMTPParsedMessage parse(final ChannelBuffer buffer) throws ZMTPMessageParsingException {

    // If we're in discarding mode, continue discarding data
    if (isOversized(size)) {
      return discardFrames(buffer);
    }

    while (buffer.readableBytes() > 0) {
      buffer.markReaderIndex();

      // Parse frame header
      final boolean parsedHeader = parseZMTPHeader(buffer);
      if (!parsedHeader) {
        // Wait for more data to decode
        buffer.resetReaderIndex();
        return null;
      }

      // Check if the message size limit is reached
      if (isOversized(size + frameSize)) {
        // Enter discarding mode
        buffer.resetReaderIndex();
        return discardFrames(buffer);
      }

      if (frameSize > buffer.readableBytes()) {
        // Wait for more data to decode
        buffer.resetReaderIndex();
        return null;
      }

      size += frameSize;

      // Read frame content
      final ZMTPFrame frame = ZMTPFrame.read(buffer, frameSize);

      frames.add(frame);

      if (!hasMore) {
        return finish(false);
      }
    }

    return null;
  }

  /**
   * Check if the message is too large and frames should be discarded.
   */
  private boolean isOversized(final long size) {
    return size > sizeLimit;
  }

  /**
   * Create a message from the parsed frames and reset the parser.
   */
  private ZMTPParsedMessage finish(final boolean truncated) {
    final ZMTPMessage message = ZMTPMessage.from(frames);
    final ZMTPParsedMessage parsedMessage = new ZMTPParsedMessage(truncated, size, message);
    reset();
    return parsedMessage;
  }

  /**
   * Reset parser in preparation for the next message.
   */
  private void reset() {
    frames = new ArrayList<ZMTPFrame>();
    hasMore = true;
    size = 0;
  }

  /**
   * Discard frames for current message.
   *
   * @return A truncated message if done discarding, null if not yet done.
   */
  private ZMTPParsedMessage discardFrames(final ChannelBuffer buffer)
      throws ZMTPMessageParsingException {

    while (buffer.readableBytes() > 0) {
      // Parse header if necessary
      if (!headerParsed) {
        buffer.markReaderIndex();
        headerParsed = parseZMTPHeader(buffer);
        if (!headerParsed) {
          // Wait for more data to decode
          buffer.resetReaderIndex();
          return null;
        }
        size += frameSize;
        frameRemaining = frameSize;
      }

      // Discard bytes
      final int discardBytes = min(frameRemaining, buffer.readableBytes());
      frameRemaining -= discardBytes;
      buffer.skipBytes(discardBytes);

      // Check if this frame is completely discarded
      final boolean done = frameRemaining == 0;
      if (done) {
        headerParsed = false;
      }

      // Check if this message is done discarding
      if (done && !hasMore) {
        // We're done discarding
        return finish(true);
      }
    }

    return null;
  }

  private boolean parseZMTPHeader(final ChannelBuffer buffer) throws ZMTPMessageParsingException {
    return version == 1 ? parseZMTP1Header(buffer) : parseZMTP2Header(buffer);
  }

  /**
   * Parse a frame header.
   */
  private boolean parseZMTP1Header(final ChannelBuffer buffer) throws ZMTPMessageParsingException {
    final long len = ZMTPUtils.decodeLength(buffer);

    if (len > Integer.MAX_VALUE) {
      throw new ZMTPMessageParsingException("Received too large frame: " + len);
    }

    if (len == -1) {
      return false;
    }

    if (len == 0) {
      throw new ZMTPMessageParsingException("Received frame with zero length");
    }

    // Read if we have more frames from flag byte
    if (buffer.readableBytes() < 1) {
      // Wait for more data to decode
      return false;
    }

    frameSize = (int) len - 1;
    hasMore = (buffer.readByte() & MORE_FLAG) == MORE_FLAG;

    return true;
  }

  private boolean parseZMTP2Header(ChannelBuffer buffer) throws ZMTPMessageParsingException {
    if (buffer.readableBytes() < 2) {
      return false;
    }
    int flags = buffer.readByte();
    hasMore = (flags & MORE_FLAG) == MORE_FLAG;
    if ((flags & LONG_FLAG) != LONG_FLAG) {
      frameSize = buffer.readByte() & 0xff;
      return true;
    }
    if (buffer.readableBytes() < 8) {
      return false;
    }
    long len;
    if (buffer.order() == BIG_ENDIAN) {
      len = buffer.readLong();
    } else {
      len = swapLong(buffer.readLong());
    }
    if (len > Integer.MAX_VALUE) {
      throw new ZMTPMessageParsingException("Received too large frame: " + len);
    }
    frameSize = (int)len;
    return true;
  }
}
