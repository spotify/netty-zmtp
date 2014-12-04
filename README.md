[![Build Status](https://travis-ci.org/spotify/netty-zmtp.png?branch=master)](https://travis-ci.org/spotify/netty-zmtp)

This is a ZeroMQ codec for Netty that aims to implement ZMTP, the ZeroMQ
Message Transport Protocol versions 1.0 and 2.0 as specified in
http://rfc.zeromq.org/spec:13 and http://rfc.zeromq.org/spec:15.

This project is hosted on https://github.com/spotify/netty-zmtp/

At Spotify we use ZeroMQ for a lot of the internal communication between
services in the backend. As we implement more services on top of the JVM we
felt the need for more control over the state of TCP connections, routing,
message queue management, etc as well as getting better performance than seems
to be currently possible with the JNI based JZMQ library.

This project implements the ZMTP wire protocol but not the ZeroMQ API, meaning
that it can be used to communicate with other peers using e.g. ZeroMQ (libzmq)
but it's not a drop-in replacement for JZMQ like e.g. JeroMQ attempts to be.

We have successfully used these handlers to implement services capable of
processing millions of messages per second.

Currently this project targets Java 6+ and Netty 3.x. It does not have any
native dependency on e.g. libzmq.

## Usage

To use netty-zmtp, insert one of `ZMTP10Codec` or `ZMTP20Codec` into your
`ChannelPipeline` and it will turn incoming buffers into  `ZMTPIncomingMessage`
instances up the pipeline and accept `ZMTPMessage` instances that gets
serialized into buffers downstream.

## Feedback

There is an open Google group for general development and usage discussion
available at https://groups.google.com/group/netty-zmtp

We use the github issue tracker at https://github.com/spotify/netty-zmtp/issues

## License

This software is licensed using the Apache 2.0 license. Details in the file
LICENSE.txt

