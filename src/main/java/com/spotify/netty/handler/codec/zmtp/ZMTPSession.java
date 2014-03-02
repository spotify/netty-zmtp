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

import io.netty.channel.Channel;

import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;
import java.util.UUID;

/**
 * Represents a ongoing zmtp session
 */
public class ZMTPSession {

    public static final int DEFAULT_SIZE_LIMIT = Integer.MAX_VALUE;

    private final boolean useLocalIdentity;
    private final byte[] localIdent;
    private final long sizeLimit;
    private final ZMTPSocketType socketType;


    private ZMTPConnectionType type;
    private Channel channel;
    private byte[] remoteIdent;
    private volatile int actualVersion;

    public ZMTPSession(final ZMTPConnectionType type) {
        this(type, Integer.MAX_VALUE);
    }

    public ZMTPSession(final ZMTPConnectionType type, final long sizeLimit) {
        this(type, sizeLimit, null, null);
    }

    public ZMTPSession(final ZMTPConnectionType type, @Nullable final byte[] localIdent) {
        this(type, DEFAULT_SIZE_LIMIT, localIdent, null);
    }

    public ZMTPSession(final ZMTPConnectionType type, final long sizeLimit,
                       @Nullable final byte[] localIdent, final ZMTPSocketType socketType) {
        this.type = type;
        this.sizeLimit = sizeLimit;
        this.useLocalIdentity = (localIdent != null);
        if (localIdent == null) {
            this.localIdent = ZMTPUtils.getBytesFromUUID(UUID.randomUUID());
        } else {
            this.localIdent = localIdent;
        }
        this.socketType = socketType;
    }

    /**
     * @return The local address of the session
     */
    public SocketAddress getLocalAddress() {
        return channel.localAddress();
    }

    /**
     * @return The remote address of the session
     */
    public SocketAddress getRemoteAddress() {
        return channel.remoteAddress();
    }

    /**
     * Type of connection dictates if a identity frame is needed
     *
     * @return Returns the type of connection
     */
    public ZMTPConnectionType getConnectionType() {
        return type;
    }

    /**
     * Changes the type of connection of the session
     */
    void setConnectionType(final ZMTPConnectionType type) {
        this.type = type;
    }

    /**
     * Helper to determine if messages in this session are enveloped
     */
    public boolean isEnveloped() {
        return (type == ZMTPConnectionType.Addressed);
    }

    /**
     * Get the remote session id (can be used for persistent queuing)
     */
    public byte[] getRemoteIdentity() {
        return remoteIdent;
    }

    /**
     * Return the local identity
     */
    public byte[] getLocalIdentity() {
        return localIdent;
    }

    /**
     * Do we have a local identity or does the system create one
     */
    public boolean useLocalIdentity() {
        return useLocalIdentity;
    }

    /**
     * Set the remote identity
     *
     * @param remoteIdent Remote identity, if null an identity will be created
     */
    public void setRemoteIdentity(@Nullable final byte[] remoteIdent) {
        if (this.remoteIdent != null) {
            throw new IllegalStateException("Remote identity already set");
        }

        this.remoteIdent = remoteIdent;
        if (this.remoteIdent == null) {
            // Create a new remote identity
            this.remoteIdent = ZMTPUtils.getBytesFromUUID(UUID.randomUUID());
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(final Channel channel) {
        this.channel = channel;
    }


    public long getSizeLimit() {
        return sizeLimit;
    }

    /**
     * An integer representing the actual version of an ZMTP connection. Note that this property
     * does not effect which protcol versions support (To pick a version, select one of the CodecBase
     * subclasses) and the actualVersion might be lower than the highest supported version of your
     * CodecBase subclass due to interoperability downgrades.
     *
     * @return 1 for ZMTP/1.0 or 2 for ZMTP/2.0.
     */
    public int getActualVersion() {
        return actualVersion;
    }

    public void setActualVersion(int actualVersion) {
        this.actualVersion = actualVersion;
    }

    public ZMTPSocketType getSocketType() {
        return socketType;
    }
}
