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

import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

@Deprecated
public class ZMTPFrame {

    public static final ZMTPFrame EMPTY_FRAME = new ZMTPFrame(new byte[]{});

    private final byte[] data;

    private ZMTPFrame(byte[] data) {
        this.data = data;
    }

    /**
     * Returns the data for a frame
     */
    @Deprecated
    public byte[] getData() {
        return hasData() ? data : null;
    }

    /**
     * Returns the length of the data
     */
    public int size() {
        return data.length;
    }

    public boolean hasData() {
        return data.length > 0;
    }

    /**
     * Create a frame from a string
     *
     * @return a frame containing the string as default byte encoding
     */
    public static ZMTPFrame create(final String data) {
        return create(data.getBytes());
    }

    /**
     * Create a new frame from a string
     *
     * @param data        String
     * @param charsetName Used to get the bytes
     * @return a ZMTP frame containing the byte encoded string
     */
    public static ZMTPFrame create(final String data, final String charsetName)
            throws UnsupportedEncodingException {
        return create(data, Charset.forName(charsetName));
    }

    /**
     * Create a new frame from a string
     *
     * @param data    String
     * @param charset Used to get the bytes
     * @return a ZMTP frame containing the byte encoded string
     */
    public static ZMTPFrame create(String data, Charset charset) {
        if (data.length() == 0) {
            return EMPTY_FRAME;
        } else {
            return new ZMTPFrame(data.getBytes(charset));
        }
    }

    /**
     * Create a new frame from a byte array.
     */
    public static ZMTPFrame create(byte[] data) {
        if (data == null || data.length == 0) {
            return EMPTY_FRAME;
        } else {
            return new ZMTPFrame(data);
        }
    }

    /**
     * Create a new frame from a channel buffer.
     */
    public static ZMTPFrame create(ByteBuf buf) {
        if (!buf.isReadable()) {
            return EMPTY_FRAME;
        } else {
            byte[] dst = new byte[buf.capacity()];
            buf.readBytes(dst);
            return new ZMTPFrame(dst);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ZMTPFrame zmtpFrame = (ZMTPFrame) o;

        if (data != null ? !data.equals(zmtpFrame.data) : zmtpFrame.data != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return data != null ? data.hashCode() : 0;
    }

    /**
     * Helper used during decoding of a ZMTP frame
     *
     * @param length length of buffer
     * @return A {@link ZMTPFrame} containg the data read from the buffer.
     */
    public static ZMTPFrame read(ByteBuf buffer, int length) {
        if (length > 0) {
            final ByteBuf data = buffer.readSlice(length);
            return create(data);
        } else {
            return EMPTY_FRAME;
        }
    }

    @Override
    public String toString() {
        return "ZMTPFrame{\"" + ZMTPUtils.toString(getData()) + "\"}";
    }
}
