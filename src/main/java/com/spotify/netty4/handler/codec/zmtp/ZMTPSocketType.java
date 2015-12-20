package com.spotify.netty4.handler.codec.zmtp;

/**
 * Enumerates the different socket types, used to make sure that connecting both peers in a socket
 * pair has compatible socket types.
 */
public enum ZMTPSocketType {
  PAIR,
  PUB,
  SUB,
  REQ,
  REP,
  DEALER,
  ROUTER,
  PULL,
  PUSH
}
