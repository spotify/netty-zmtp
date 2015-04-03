package com.spotify.netty4.handler.codec.zmtp;

/**
 * Enumerates the different socket types, used to make sure that connecting both peers in a socket
 * pair has compatible socket types.
 */
public enum ZMTPSocketType {

  PAIR(false),
  SUB(false),
  PUB(false),
  REQ(true),
  REP(true),
  DEALER(true),
  ROUTER(true),
  PULL(false),
  PUSH(false),
  UNKNOWN(false);

  private final boolean enveloped;

  ZMTPSocketType(final boolean enveloped) {
    this.enveloped = enveloped;
  }

  public boolean isEnveloped() {
    return enveloped;
  }
}
