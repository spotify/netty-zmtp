package com.spotify.netty.handler.codec.zmtp;

/**
 * This interface is implemented by classes that wants to receive notifications about
 * the completion of a ZMTP handshake
 */
interface HandshakeListener {
    void handshakeDone(int protocolVersion, byte[] remoteIdentity);
}
