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

import com.spotify.netty4.handler.AutoFlusher;
import com.spotify.netty4.handler.codec.zmtp.ZMTP10Codec;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageEncoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPSession;
import com.spotify.netty4.handler.codec.zmtp.ZMTPWriter;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.chmv8.ForkJoinPool;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPConnectionType.Addressed;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSession.DEFAULT_SIZE_LIMIT;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static com.spotify.netty4.handler.codec.zmtp.benchmarks.AsciiString.ASCII_STRING_FROM_STRING;
import static io.netty.util.CharsetUtil.UTF_8;
import static java.util.Arrays.asList;

public class ApplicationBenchmark {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private static final byte[] NO_IDENTITY = null;

  private static final boolean INTEROP_ON = true;
  private static final boolean INTEROP_OFF = false;

  private static final InetSocketAddress ANY_PORT = new InetSocketAddress("127.0.0.1", 0);
  public static final Thread.UncaughtExceptionHandler
      UNCAUGHT_EXCEPTION_HANDLER =
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(final Thread thread, final Throwable throwable) {
          throwable.printStackTrace();
        }
      };

  public static void main(final String... args) throws InterruptedException {
    final ProgressMeter meter = new ProgressMeter("requests");

    // Codecs
    final ZMTP10Codec serverCodec = new ZMTP10Codec(
        new ZMTPSession(Addressed, DEFAULT_SIZE_LIMIT, NO_IDENTITY, ROUTER),
        new ReplyEncoder(),
        new RequestDecoder());

    final ZMTP10Codec clientCodec = new ZMTP10Codec(
        new ZMTPSession(Addressed, DEFAULT_SIZE_LIMIT, NO_IDENTITY, DEALER),
        new RequestEncoder(),
        new ReplyDecoder());

    // Server
    final Executor serverExecutor = new ForkJoinPool(
        1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, UNCAUGHT_EXCEPTION_HANDLER, true);
    final ServerBootstrap serverBootstrap = new ServerBootstrap()
        .group(new NioEventLoopGroup(1), new NioEventLoopGroup())
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childHandler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(final NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(serverCodec);
            ch.pipeline().addLast(new ServerRequestTracker());
            ch.pipeline().addLast(ImmediateEventExecutor.INSTANCE, new AutoFlusher());
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
        .handler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(final NioSocketChannel ch) throws Exception {
            ch.pipeline().addLast(clientCodec);
            ch.pipeline().addLast(new ClientRequestTracker());
            ch.pipeline().addLast(ImmediateEventExecutor.INSTANCE, new AutoFlusher());
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

    public ServerHandler(final Executor executor) {
      this.executor = executor;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          final Request request = (Request) msg;
          ctx.write(request.reply(200, REPLY_PAYLOAD));
        }
      });
    }
  }

  private static class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final int CONCURRENCY = 1000;

    private final ProgressMeter meter;
    private final Executor executor;

    private ChannelHandlerContext ctx;

    public ClientHandler(final ProgressMeter meter, final Executor executor) {
      this.meter = meter;
      this.executor = executor;
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
      super.channelRegistered(ctx);
      this.ctx = ctx;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      for (int i = 0; i < CONCURRENCY; i++) {
        send();
      }
    }

    private void send() {
      final RequestPromise promise = new RequestPromise(ctx.channel());
      ctx.write(req(), promise);
      Futures.addCallback(promise.replyFuture(), new FutureCallback<Reply>() {
        @Override
        public void onSuccess(final Reply reply) {
          final long latency = System.nanoTime() - reply.id().timestamp();
          meter.inc(1, latency);
          send();
        }

        @Override
        public void onFailure(final Throwable t) {
          System.err.println("failure");
        }
      }, executor);
    }

    private Request req() {
      final MessageId id = MessageId.generate();
      return new Request(id, "foo://bar/some/resource", "GET", EMPTY_BUFFER);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          ctx.write(req());
        }
      });
    }
  }

  private static class RequestPromise extends DefaultChannelPromise {

    private final SettableFuture<Reply> replyFuture = SettableFuture.create();

    public RequestPromise(final Channel channel) {
      super(channel);
    }

    public RequestPromise(final Channel channel, final EventExecutor executor) {
      super(channel, executor);
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

    public static MessageId generate() {
      return new MessageId(ThreadLocalRandom.current().nextLong(), System.nanoTime());
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final MessageId messageId = (MessageId) o;

      if (seq != messageId.seq) {
        return false;
      }
      if (timestamp != messageId.timestamp) {
        return false;
      }

      return true;
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

  private static class RequestEncoder implements ZMTPMessageEncoder {

    @Override
    public void encode(final Object message, final ZMTPWriter writer) {
      final Request request = (Request) message;

      writer.expectFrame(request.uri().length());
      writer.expectFrame(request.method().length());
      writer.expectFrame(16);
      writer.expectFrame(request.payload().remaining());

      writer.begin();

      writeAscii(writer, request.uri());
      writeAscii(writer, request.method());
      writeId(writer, request.id());
      writePayload(writer, request.payload());

      writer.end();
    }
  }

  private static class ReplyEncoder implements ZMTPMessageEncoder {

    @Override
    public void encode(final Object message, final ZMTPWriter writer) {
      final Reply reply = (Reply) message;
      writer.expectFrame(reply.uri().length());
      writer.expectFrame(reply.method().length());
      writer.expectFrame(16);
      writer.expectFrame(4);
      writer.expectFrame(reply.payload().remaining());

      writer.begin();

      writeAscii(writer, reply.uri());
      writeAscii(writer, reply.method());
      writeId(writer, reply.id());
      writer.frame(4).writeInt(reply.statusCode());
      writePayload(writer, reply.payload());

      writer.end();
    }

  }

  private static void writeAscii(final ZMTPWriter writer, final CharSequence s) {
    final ByteBuf frame = writer.frame(s.length());
    if (s instanceof AsciiString) {
      ((AsciiString) s).write(frame);
    } else {
      for (int i = 0; i < s.length(); i++) {
        frame.writeByte(s.charAt(i));
      }
    }
  }

  private static void writeId(final ZMTPWriter writer, final MessageId id) {
    writer.frame(16)
        .writeLong(id.seq())
        .writeLong(id.timestamp());
  }

  private static void writePayload(final ZMTPWriter writer, final ByteBuffer payload) {
    final ByteBuf buf = writer.frame(payload.remaining());
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

  private static AsciiString[] METHODS = FluentIterable
      .from(asList("GET", "POST", "PUT", "DELETE", "PATCH"))
      .transform(ASCII_STRING_FROM_STRING)
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

  private static class RequestDecoder implements ZMTPMessageDecoder {

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

    @Override
    public void readFrame(final ByteBuf data, final int size, final boolean more) {
      switch (state) {
        case URI:
          uri = readAscii(data, size);
          state = State.METHOD;
          break;
        case METHOD:
          method = readMethod(data, size);
          state = State.ID;
          break;
        case ID:
          id = readId(data, size);
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          payload = readPayload(data, size);
          state = State.URI;
          break;
      }
    }

    @Override
    public void discardFrame(final int size, final boolean more) {
      switch (state) {
        case URI:
          state = State.METHOD;
          break;
        case METHOD:
          state = State.ID;
          break;
        case ID:
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          state = State.URI;
          break;
      }
    }

    @Override
    public Request finish() {
      return new Request(id, uri, method, payload);
    }
  }

  private static class ReplyDecoder implements ZMTPMessageDecoder {

    enum State {
      URI,
      METHOD,
      ID,
      STATUSCODE,
      PAYLOAD
    }

    private State state = State.URI;

    private CharSequence uri;
    private CharSequence method;
    private MessageId id;
    private int statusCode;
    private ByteBuffer payload;

    @Override
    public void readFrame(final ByteBuf data, final int size, final boolean more) {
      switch (state) {
        case URI:
          uri = readAscii(data, size);
          state = State.METHOD;
          break;
        case METHOD:
          method = readMethod(data, size);
          state = State.ID;
          break;
        case ID:
          id = readId(data, size);
          state = State.STATUSCODE;
          break;
        case STATUSCODE:
          statusCode = readStatusCode(data, size);
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          payload = readPayload(data, size);
          state = State.URI;
          break;
      }
    }

    @Override
    public void discardFrame(final int size, final boolean more) {
      switch (state) {
        case URI:
          state = State.METHOD;
          break;
        case METHOD:
          state = State.STATUSCODE;
          break;
        case STATUSCODE:
          state = State.ID;
          break;
        case ID:
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          state = State.URI;
          break;
      }
    }

    @Override
    public Reply finish() {
      return new Reply(id, uri, method, statusCode, payload);
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
}
