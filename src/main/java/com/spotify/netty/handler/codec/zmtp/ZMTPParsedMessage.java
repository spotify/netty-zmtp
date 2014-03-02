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

public class ZMTPParsedMessage {

    private final boolean truncated;
    private final long byteSize;
    private final ZMTPMessage message;

    public ZMTPParsedMessage(final boolean truncated, final long byteSize,
                             final ZMTPMessage message) {
        this.truncated = truncated;
        this.byteSize = byteSize;
        this.message = message;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public long getByteSize() {
        return byteSize;
    }

    public ZMTPMessage getMessage() {
        return message;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ZMTPParsedMessage that = (ZMTPParsedMessage) o;

        if (byteSize != that.byteSize) {
            return false;
        }
        if (truncated != that.truncated) {
            return false;
        }
        if (message != null ? !message.equals(that.message) : that.message != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (truncated ? 1 : 0);
        result = 31 * result + (int) (byteSize ^ (byteSize >>> 32));
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ZMTPParsedMessage{" +
                "truncated=" + truncated +
                ", byteSize=" + byteSize +
                ", message=" + message +
                '}';
    }
}
