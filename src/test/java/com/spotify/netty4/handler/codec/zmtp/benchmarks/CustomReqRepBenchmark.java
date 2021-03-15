/*
 * Copyright (c) 2012-2014 Spotify AB
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

package com.spotify.netty4.handler.codec.zmtp.benchmarks;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.netty4.handler.codec.zmtp.ZMTPCodec;
import com.spotify.netty4.handler.codec.zmtp.ZMTPDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPEncoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPEstimator;
import com.spotify.netty4.handler.codec.zmtp.ZMTPHandshakeSuccess;
import com.spotify.netty4.handler.codec.zmtp.ZMTPWriter;
import com.spotify.netty4.util.BatchFlusher;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;

public class CustomReqRepBenchmark {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);
  private static final Thread.UncaughtExceptionHandler
      UNCAUGHT_EXCEPTION_HANDLER =
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(final Thread thread, final Throwable throwable) {
          throwable.printStackTrace();
        }
      };

  public static void main(final String... args) throws InterruptedException {
    final ProgressMeter meter = new ProgressMeter("requests", true);

    // Codecs
    final ZMTPCodec serverCodec = ZMTPCodec.builder()
        .socketType(ROUTER)
        .encoder(ReplyEncoder.class)
        .decoder(RequestDecoder.class)
        .build();

    final ZMTPCodec clientCodec = ZMTPCodec.builder()
        .socketType(DEALER)
        .encoder(RequestEncoder.class)
        .decoder(ReplyDecoder.class)
        .build();

    // Server
    final Executor serverExecutor = new ForkJoinPool(
        1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, UNCAUGHT_EXCEPTION_HANDLER, true);
    final ServerBootstrap serverBootstrap = new ServerBootstrap()
        .group(new NioEventLoopGroup(1), new NioEventLoopGroup())
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.MESSAGE_SIZE_ESTIMATOR, ByteBufSizeEstimator.INSTANCE)
        .childHandler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(final NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(serverCodec);
            ch.pipeline().addLast(new ServerRequestTracker());
            ch.pipeline().addLast(new ServerHandler(serverExecutor));
          }
        });
    final Channel server = serverBootstrap.bind(ANY_PORT).awaitUninterruptibly().channel();

    // Client
    final Executor clientExecutor = new ForkJoinPool(
        1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, UNCAUGHT_EXCEPTION_HANDLER, true);
    final SocketAddress address = server.localAddress();
    final Bootstrap clientBootstrap = new Bootstrap()
        .group(new NioEventLoopGroup())
        .channel(NioSocketChannel.class)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, ByteBufSizeEstimator.INSTANCE)
        .handler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(final NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(clientCodec);
            ch.pipeline().addLast(new ClientRequestTracker());
            ch.pipeline().addLast(new ClientHandler(meter, clientExecutor));
          }
        });
    final Channel client = clientBootstrap.connect(address).awaitUninterruptibly().channel();

    // Run until client is closed
    client.closeFuture().await();
  }

  private static class ServerHandler extends ChannelInboundHandlerAdapter {

    public static final ByteBuffer REPLY_PAYLOAD = UTF_8.encode("hello world");

    private final Executor executor;

    private BatchFlusher flusher;

    public ServerHandler(final Executor executor) {
      this.executor = executor;
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
      super.channelRegistered(ctx);
      this.flusher = new BatchFlusher(ctx.channel());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          final Request request = (Request) msg;
          ctx.write(request.reply(200, REPLY_PAYLOAD));
          flusher.flush();
        }
      });
    }
  }

  private static class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final int CONCURRENCY = 1000;

    private final ProgressMeter meter;
    private final Executor executor;

    private BatchFlusher flusher;
    private ChannelHandlerContext ctx;

    private long seq = new SecureRandom().nextLong();

    public ClientHandler(final ProgressMeter meter, final Executor executor) {
      this.meter = meter;
      this.executor = executor;
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
      this.ctx = ctx;
      this.flusher = new BatchFlusher(ctx.channel());
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
        throws Exception {
      if (evt instanceof ZMTPHandshakeSuccess) {
        for (int i = 0; i < CONCURRENCY; i++) {
          send(ctx);
        }
      }
    }

    private void send(final ChannelHandlerContext ctx) {
      final RequestPromise promise = new RequestPromise(this.ctx.channel());
      ctx.write(req(), promise);
      flusher.flush();
      Futures.addCallback(promise.replyFuture(), new FutureCallback<Reply>() {
        @Override
        public void onSuccess(final Reply reply) {
          final long latency = System.nanoTime() - reply.id().timestamp();
          meter.inc(1, latency);
          send(ctx);
        }

        @Override
        public void onFailure(final Throwable t) {
          System.err.println("failure");
        }
      }, executor);
    }

    public static long rand(long x) {
      x ^= (x << 21);
      x ^= (x >>> 35);
      x ^= (x << 4);
      return x;
    }

    private Request req() {
      seq = rand(seq);
      final MessageId id = new MessageId(seq, System.nanoTime());
      return new Request(id, "foo://bar/some/resource", "GET", EMPTY_BUFFER);
    }
  }

  private static class RequestPromise extends DefaultChannelPromise {

    private final SettableFuture<Reply> replyFuture = SettableFuture.create();

    public RequestPromise(final Channel channel) {
      super(channel);
    }

    public SettableFuture<Reply> replyFuture() {
      return replyFuture;
    }
  }

  private static class Message {

    private final MessageId id;
    private final CharSequence uri;
    private final CharSequence method;
    private final ByteBuffer payload;

    public Message(final MessageId id, final CharSequence method,
                   final CharSequence uri,
                   final ByteBuffer payload) {
      this.id = id;
      this.method = method;
      this.uri = uri;
      this.payload = payload;
    }

    public MessageId id() {
      return id;
    }

    public CharSequence uri() {
      return uri;
    }

    public CharSequence method() {
      return method;
    }

    public ByteBuffer payload() {
      return payload;
    }
  }

  private static class Request extends Message {

    public Request(final MessageId id, final CharSequence uri, final CharSequence method,
                   final ByteBuffer payload) {
      super(id, method, uri, payload);
    }

    public Reply reply(final int code, final ByteBuffer payload) {
      return new Reply(id(), uri(), method(), code, payload);
    }
  }

  private static class Reply extends Message {

    private final int code;

    public Reply(final MessageId id, final CharSequence uri, final CharSequence method,
                 final int code,
                 final ByteBuffer payload) {
      super(id, method, uri, payload);
      this.code = code;
    }

    public int statusCode() {
      return code;
    }
  }

  private static class MessageId {

    private final long seq;
    private final long timestamp;

    public MessageId(final long seq, final long timestamp) {
      this.seq = seq;
      this.timestamp = timestamp;
    }

    public long seq() {
      return seq;
    }

    public long timestamp() {
      return timestamp;
    }

    public static MessageId from(final long seq, final long timestamp) {
      return new MessageId(seq, timestamp);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }

      final MessageId messageId = (MessageId) o;

      if (seq != messageId.seq) { return false; }
      return timestamp == messageId.timestamp;
    }

    @Override
    public int hashCode() {
      int result = (int) (seq ^ (seq >>> 32));
      result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "MessageId{" +
             "seq=" + seq +
             ", timestamp=" + timestamp +
             '}';
    }
  }

  private static class RequestEncoder implements ZMTPEncoder {

    @Override
    public void estimate(final Object message, final ZMTPEstimator estimator) {
      final Request request = (Request) message;
      estimator.frame(request.uri().length());
      estimator.frame(request.method().length());
      estimator.frame(16);
      estimator.frame(request.payload().remaining());
    }

    @Override
    public void encode(final Object message, final ZMTPWriter writer) {
      final Request request = (Request) message;
      writeAscii(writer, request.uri());
      writeAscii(writer, request.method());
      writeId(writer, request.id());
      writePayload(writer, request.payload());
    }

    @Override
    public void close() {
    }
  }

  private static class ReplyEncoder implements ZMTPEncoder {

    @Override
    public void estimate(final Object message, final ZMTPEstimator estimator) {
      final Reply reply = (Reply) message;
      estimator.frame(reply.uri().length());
      estimator.frame(reply.method().length());
      estimator.frame(16);
      estimator.frame(4);
      estimator.frame(reply.payload().remaining());
    }

    @Override
    public void encode(final Object message, final ZMTPWriter writer) {
      final Reply reply = (Reply) message;
      writeAscii(writer, reply.uri());
      writeAscii(writer, reply.method());
      writeId(writer, reply.id());
      writer.frame(4, true).writeInt(reply.statusCode());
      writePayload(writer, reply.payload());
    }

    @Override
    public void close() {
    }
  }

  private static void writeAscii(final ZMTPWriter writer, final CharSequence s) {
    final ByteBuf frame = writer.frame(s.length(), true);
    if (s instanceof AsciiString) {
      ((AsciiString) s).write(frame);
    } else {
      for (int i = 0; i < s.length(); i++) {
        frame.writeByte(s.charAt(i));
      }
    }
  }

  private static void writeId(final ZMTPWriter writer, final MessageId id) {
    writer.frame(16, true)
        .writeLong(id.seq())
        .writeLong(id.timestamp());
  }

  private static void writePayload(final ZMTPWriter writer, final ByteBuffer payload) {
    final ByteBuf buf = writer.frame(payload.remaining(), false);
    if (payload.hasArray()) {
      buf.writeBytes(payload.array(), payload.arrayOffset() + payload.position(),
                     payload.remaining());
    } else {
      final int pos = payload.position();
      for (int i = 0; i < payload.remaining(); i++) {
        buf.writeByte(payload.get(pos + i));
      }
      payload.position(pos);
    }
  }

  private static ByteBuffer readPayload(final ByteBuf data, final int size) {
    if (size == 0) {
      return EMPTY_BUFFER;
    }
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    data.readBytes(buffer);
    buffer.flip();
    return buffer;
  }

  private static int readStatusCode(final ByteBuf data, final int size) {
    if (size != 4) {
      throw new IllegalArgumentException();
    }
    return data.readInt();
  }

  private static CharSequence readAscii(final ByteBuf data, final int size) {
    final byte[] chars = new byte[size];
    data.readBytes(chars);
    return new AsciiString(chars);
  }

  private static final AsciiString[] METHODS = FluentIterable
      .from(asList("GET", "POST", "PUT", "DELETE", "PATCH"))
      .transform(AsciiString.ASCII_STRING_FROM_STRING)
      .toArray(AsciiString.class);

  private static CharSequence readMethod(final ByteBuf data, final int size) {
    for (final AsciiString method : METHODS) {
      if (asciiEquals(method, data, size)) {
        data.skipBytes(size);
        return method;
      }
    }
    return readAscii(data, size);
  }

  private static boolean asciiEquals(final AsciiString s, final ByteBuf data, final int size) {
    final int ix = data.readerIndex();
    if (size != s.length()) {
      return false;
    }
    for (int i = 0; i < size; i++) {
      char c = (char) data.getByte(ix + i);
      if (c != s.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  private static MessageId readId(final ByteBuf data, final int size) {
    if (size != 16) {
      throw new IllegalArgumentException();
    }
    final long seq = data.readLong();
    final long timestamp = data.readLong();
    return MessageId.from(seq, timestamp);
  }

  private static class RequestDecoder implements ZMTPDecoder {

    enum State {
      URI,
      METHOD,
      ID,
      PAYLOAD
    }

    private State state = State.URI;

    private CharSequence uri;
    private CharSequence method;
    private MessageId id;
    private ByteBuffer payload;

    private int frameLength;

    @Override
    public void header(final ChannelHandlerContext ctx, final long length, final boolean more,
                       final List<Object> out) {
      if (length > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("length");
      }
      frameLength = (int) length;
    }

    @Override
    public void content(final ChannelHandlerContext ctx, final ByteBuf data,
                        final List<Object> out) {
      if (data.readableBytes() < frameLength) {
        return;
      }
      switch (state) {
        case URI:
          uri = readAscii(data, frameLength);
          state = State.METHOD;
          break;
        case METHOD:
          method = readMethod(data, frameLength);
          state = State.ID;
          break;
        case ID:
          id = readId(data, frameLength);
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          payload = readPayload(data, frameLength);
          state = State.URI;
          break;
      }
    }

    @Override
    public void finish(final ChannelHandlerContext ctx, final List<Object> out) {
      out.add(new Request(id, uri, method, payload));
    }

    @Override
    public void close() {
    }
  }

  private static class ReplyDecoder implements ZMTPDecoder {

    enum State {
      URI,
      METHOD,
      ID,
      STATUSCODE,
      PAYLOAD
    }

    private State state = State.URI;
    private int frameLength;

    private CharSequence uri;
    private CharSequence method;
    private MessageId id;
    private int statusCode;
    private ByteBuffer payload;

    @Override
    public void header(final ChannelHandlerContext ctx, final long length, final boolean more,
                       final List<Object> out) {
      if (length > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("length");
      }
      frameLength = (int) length;
    }

    @Override
    public void content(final ChannelHandlerContext ctx, final ByteBuf data,
                        final List<Object> out) {
      if (data.readableBytes() < frameLength) {
        return;
      }
      switch (state) {
        case URI:
          uri = readAscii(data, frameLength);
          state = State.METHOD;
          break;
        case METHOD:
          method = readMethod(data, frameLength);
          state = State.ID;
          break;
        case ID:
          id = readId(data, frameLength);
          state = State.STATUSCODE;
          break;
        case STATUSCODE:
          statusCode = readStatusCode(data, frameLength);
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          payload = readPayload(data, frameLength);
          state = State.URI;
          break;
      }
    }

    @Override
    public void finish(final ChannelHandlerContext ctx, final List<Object> out) {
      out.add(new Reply(id, uri, method, statusCode, payload));
    }

    @Override
    public void close() {
    }
  }

  private static class ServerRequestTracker extends ChannelDuplexHandler {

    private final Map<MessageId, Request> outstanding = Maps.newHashMap();

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
                      final ChannelPromise promise)
        throws Exception {
      final Reply reply = (Reply) msg;
      final Request request = outstanding.remove(reply.id());
      if (request == null) {
        System.err.println("Unexpected reply: " + reply);
      } else {
        super.write(ctx, msg, promise);
      }
    }

    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final Request request = (Request) msg;
      outstanding.put(request.id(), request);
      super.channelRead(ctx, msg);
    }
  }

  private static class ClientRequestTracker extends ChannelDuplexHandler {

    private final Map<MessageId, SettableFuture<Reply>> outstanding = Maps.newHashMap();

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
                      final ChannelPromise promise)
        throws Exception {
      final Request request = (Request) msg;
      final RequestPromise requestPromise = (RequestPromise) promise;
      outstanding.put(request.id(), requestPromise.replyFuture);
      super.write(ctx, msg, promise);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final Reply reply = (Reply) msg;
      final SettableFuture<Reply> future = outstanding.remove(reply.id());
      if (future == null) {
        System.err.println("unexpected reply: " + reply);
      } else {
        future.set(reply);
      }
    }
  }

  private static class ByteBufSizeEstimator implements MessageSizeEstimator,
                                                       MessageSizeEstimator.Handle {

    public static final ByteBufSizeEstimator INSTANCE = new ByteBufSizeEstimator();

    @Override
    public Handle newHandle() {
      return this;
    }

    @Override
    public int size(final Object msg) {
      if (msg instanceof ByteBuf) {
        return ((ByteBuf) msg).readableBytes();
      }
      return 0;
    }
  }
}
