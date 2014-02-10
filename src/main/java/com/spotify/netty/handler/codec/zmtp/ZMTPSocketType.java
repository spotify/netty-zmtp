package com.spotify.netty.handler.codec.zmtp;

/**
 * Enumerates the different socket types, used to make sure that connecting
 * both peers in a socket pair has compatible socket types.
 *
 * Please note that the types needs to be the same order as defined in the ZMTP/2.0 spec so that
 * ordinal() can be used to extract serialize the type.
 *
 */
public enum ZMTPSocketType {
  PAIR,
  SUB,
  PUB,
  REQ,
  REP,
  DEALER,
  ROUTER,
  PULL,
  PUSH
}
