This is a ZeroMQ codec for Netty that aims to implement ZMTP/1.0, the ZeroMQ
Message Transport Protocol.

http://rfc.zeromq.org/spec:13

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