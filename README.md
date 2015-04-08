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

Currently this project targets Java 6+ and Netty 4.x. It does not have any
native dependency on e.g. libzmq.

## Usage

To use netty-zmtp, insert a `ZMTPCodec` instance into your channel pipeline.

```java
ch.pipeline().addLast(ZMTPCodec.of(ROUTER));
```

Upstream handlers will receive `ZMTPMessage` instances.

```java
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
 final ZMTPMessage message = (ZMTPMessage) msg;
 // ...
}
```

Wait for the ZMTP handshake to complete before sending messages.

```java
@Override
public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
    throws Exception {
  if (evt instanceof ZMTPHandshakeSuccess) {
    // ...
  }
}
```

### `pom.xml`

```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>netty4-zmtp</artifactId>
  <version>0.4.0</version>
</dependency>
```

## Benchmarks

### Preparation

First compile and run tests:

```
mvn clean test
```

Fetch dependencies:

```
mvn dependency:copy-dependencies
```

Now benchmarks are ready to run.

### One-Way Throughput

```
java -cp 'target/classes:target/test-classes:target/dependency/*' \
     com.spotify.netty4.handler.codec.zmtp.benchmarks.ThroughputBenchmark
```

```
1s:    3,393,219 messages/s.    (total:    3,399,036)
2s:    6,595,711 messages/s.    (total:   10,007,250)
3s:    7,158,176 messages/s.    (total:   17,154,830)
4s:    7,313,554 messages/s.    (total:   24,461,521)
5s:    7,294,709 messages/s.    (total:   31,790,260)
6s:    7,282,308 messages/s.    (total:   39,064,152)
7s:    7,294,591 messages/s.    (total:   46,336,682)
```


### Req/Rep Throughput


```
java -cp 'target/classes:target/test-classes:target/dependency/*' \
     com.spotify.netty4.handler.codec.zmtp.benchmarks.ReqRepBenchmark
```

```
1s:      444,848 requests/s.    (total:      446,249)
2s:    1,251,304 requests/s.    (total:    1,699,916)
3s:    1,241,569 requests/s.    (total:    2,941,709)
4s:    1,365,408 requests/s.    (total:    4,307,949)
5s:    1,379,640 requests/s.    (total:    5,681,522)
6s:    1,379,048 requests/s.    (total:    7,064,183)
7s:    1,377,180 requests/s.    (total:    8,438,673)
```

### Req/Rep With Custom Encoder/Decoder Throughput

```
java -cp 'target/classes:target/test-classes:target/dependency/*' \
     com.spotify.netty4.handler.codec.zmtp.benchmarks.ApplicationBenchmark
```

```
1s:      494,978 requests/s.         1.000 ms avg latency.    (total:      497,512)
2s:    1,345,494 requests/s.         0.000 ms avg latency.    (total:    1,838,109)
3s:    1,594,242 requests/s.         0.000 ms avg latency.    (total:    3,436,276)
4s:    1,688,887 requests/s.         0.000 ms avg latency.    (total:    5,125,976)
5s:    1,710,013 requests/s.         0.000 ms avg latency.    (total:    6,838,872)
6s:    1,707,321 requests/s.         0.000 ms avg latency.    (total:    8,542,567)
7s:    1,721,369 requests/s.         0.000 ms avg latency.    (total:   10,259,305)
```

## Feedback

There is an open Google group for general development and usage discussion
available at https://groups.google.com/group/netty-zmtp

We use the github issue tracker at https://github.com/spotify/netty-zmtp/issues

## License

This software is licensed using the Apache 2.0 license. Details in the file
LICENSE.txt
