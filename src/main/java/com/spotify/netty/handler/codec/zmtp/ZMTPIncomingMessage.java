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

/**
 * A ZMTP message along with the session it was received on.
 */
public class ZMTPIncomingMessage {

    private final ZMTPSession session;
    private final ZMTPMessage message;
    private final boolean truncated;
    private final long byteSize;

    public ZMTPIncomingMessage(final ZMTPSession session, final ZMTPMessage message,
                               final boolean truncated, final long byteSize) {
        this.session = session;
        this.message = message;
        this.truncated = truncated;
        this.byteSize = byteSize;
    }

    /**
     * Return the session this message was received on.
     *
     * @return The session this message was received on.
     */
    public ZMTPSession getSession() {
        return session;
    }

    /**
     * Get the total size in bytes of the message, including truncated frames.
     */
    public long getByteSize() {
        return byteSize;
    }

    /**
     * Return the message.
     *
     * @return The message.
     */
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

        final ZMTPIncomingMessage that = (ZMTPIncomingMessage) o;

        if (message != null ? !message.equals(that.message) : that.message != null) {
            return false;
        }
        if (session != null ? !session.equals(that.session) : that.session != null) {
            return false;
        }

        return true;
    }

    public boolean isTruncated() {
        return truncated;
    }

    @Override
    public int hashCode() {
        int result = session != null ? session.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ZMTPIncomingMessage{" +
                "session=" + session +
                ", message=" + message +
                '}';
    }
}
