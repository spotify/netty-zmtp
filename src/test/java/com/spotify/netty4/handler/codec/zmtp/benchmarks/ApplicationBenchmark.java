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

import com.spotify.netty4.handler.AutoFlusher;
import com.spotify.netty4.handler.codec.zmtp.ZMTP10Codec;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageDecoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPMessageEncoder;
import com.spotify.netty4.handler.codec.zmtp.ZMTPSession;
import com.spotify.netty4.handler.codec.zmtp.ZMTPWriter;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.chmv8.ForkJoinPool;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPConnectionType.Addressed;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSession.DEFAULT_SIZE_LIMIT;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static io.netty.util.CharsetUtil.UTF_8;

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
    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(new NioEventLoopGroup(1), new NioEventLoopGroup());
    serverBootstrap.channel(NioServerSocketChannel.class);
    serverBootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(serverCodec);
        ch.pipeline().addLast(ImmediateEventExecutor.INSTANCE, new AutoFlusher());
        ch.pipeline().addLast(new ServerHandler(serverExecutor));
      }
    });
    final Channel server = serverBootstrap.bind(ANY_PORT).awaitUninterruptibly().channel();

    // Client
    final Executor clientExecutor = new ForkJoinPool(
        1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, UNCAUGHT_EXCEPTION_HANDLER, true);
    final SocketAddress address = server.localAddress();
    final Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap.group(new NioEventLoopGroup());
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      protected void initChannel(final NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(clientCodec);
        ch.pipeline().addLast(ImmediateEventExecutor.INSTANCE, new AutoFlusher());
        ch.pipeline().addLast(new ClientHandler(meter, clientExecutor));
      }
    });
    final Channel client = clientBootstrap.connect(address).awaitUninterruptibly().channel();

    // Run until client is closed
    client.closeFuture().await();
  }

  private static class ServerHandler extends ChannelInboundHandlerAdapter {

    private final Executor executor;

    public ServerHandler(final Executor executor) {
      this.executor = executor;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final Request request = (Request) msg;
      executor.execute(new Runnable() {
        @Override
        public void run() {
          ctx.write(request.reply(200, UTF_8.encode("hello world")));
        }
      });
    }
  }

  private static class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final int CONCURRENCY = 1000;

    private final ProgressMeter meter;
    private final Executor executor;

    public ClientHandler(final ProgressMeter meter, final Executor executor) {
      this.meter = meter;
      this.executor = executor;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      for (int i = 0; i < CONCURRENCY; i++) {
        ctx.write(req());
      }
    }

    private Request req() {
      final long timestamp = System.nanoTime();
      return new Request("foo://bar/some/resource", "GET", timestamp, EMPTY_BUFFER);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final Reply reply = (Reply) msg;
      final long latency = System.nanoTime() - reply.timestamp();
      meter.inc(1, latency);
      executor.execute(new Runnable() {
        @Override
        public void run() {
          ctx.write(req());
        }
      });
    }
  }

  private static class Message {

    private final String uri;
    private final String method;
    private final long timestamp;
    private final ByteBuffer payload;

    public Message(final String method, final String uri, final long timestamp,
                   final ByteBuffer payload) {
      this.method = method;
      this.uri = uri;
      this.timestamp = timestamp;
      this.payload = payload;
    }

    public String uri() {
      return uri;
    }

    public String method() {
      return method;
    }

    public long timestamp() {
      return timestamp;
    }

    public ByteBuffer payload() {
      return payload;
    }
  }

  private static class Request extends Message {

    public Request(final String uri, final String method, final long timestamp,
                   final ByteBuffer payload) {
      super(method, uri, timestamp, payload);
    }

    public Reply reply(final int code, final ByteBuffer payload) {
      return new Reply(uri(), method(), code, timestamp(), payload);
    }
  }

  private static class Reply extends Message {

    private final int code;

    public Reply(final String uri, final String method, final int code,
                 final long timestamp,
                 final ByteBuffer payload) {
      super(method, uri, timestamp, payload);
      this.code = code;
    }

    public int statusCode() {
      return code;
    }
  }

  private static class RequestEncoder implements ZMTPMessageEncoder {

    @Override
    public void encode(final Object message, final ZMTPWriter writer) {
      final Request request = (Request) message;
      writer.expectFrame(request.uri().length());
      writer.expectFrame(request.method().length());
      writer.expectFrame(8);
      writer.expectFrame(request.payload().remaining());

      writer.begin();

      final String uri = request.uri();
      final ByteBuf uriFrame = writer.frame(uri.length());
      for (int i = 0; i < uri.length(); i++) {
        uriFrame.writeByte(uri.charAt(0));
      }

      final String method = request.method();
      final ByteBuf methodFrame = writer.frame(method.length());
      for (int i = 0; i < method.length(); i++) {
        methodFrame.writeByte(method.charAt(0));
      }

      writer.frame(8)
          .writeLong(request.timestamp());

      writer.frame(request.payload().remaining())
          .writeBytes(request.payload().slice());

      writer.end();
    }
  }

  private static class ReplyEncoder implements ZMTPMessageEncoder {

    @Override
    public void encode(final Object message, final ZMTPWriter writer) {
      final Reply reply = (Reply) message;
      writer.expectFrame(reply.uri().length());
      writer.expectFrame(reply.method().length());
      writer.expectFrame(4);
      writer.expectFrame(8);
      writer.expectFrame(reply.payload().remaining());

      writer.begin();

      final String uri = reply.uri();
      final ByteBuf uriFrame = writer.frame(uri.length());
      for (int i = 0; i < uri.length(); i++) {
        uriFrame.writeByte(uri.charAt(0));
      }

      final String method = reply.method();
      final ByteBuf methodFrame = writer.frame(method.length());
      for (int i = 0; i < method.length(); i++) {
        methodFrame.writeByte(method.charAt(0));
      }

      writer.frame(4)
          .writeInt(reply.statusCode());

      writer.frame(8)
          .writeLong(reply.timestamp());

      writer.frame(reply.payload().remaining())
          .writeBytes(reply.payload().slice());

      writer.end();
    }
  }

  private static ByteBuffer readByteBuffer(final ByteBuf data, final int size) {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    data.readBytes(buffer);
    buffer.flip();
    return buffer;
  }

  private static long readLong(final ByteBuf data, final int size) {
    if (size != 8) {
      throw new IllegalArgumentException();
    }
    return data.readLong();
  }

  private static int readInt(final ByteBuf data, final int size) {
    if (size != 4) {
      throw new IllegalArgumentException();
    }
    return data.readInt();
  }

  private static String readString(final ByteBuf data, final int size) {
    final ByteBuffer uriBuffer = data.nioBuffer(data.readerIndex(), size);
    final String string = UTF_8.decode(uriBuffer).toString();
    data.skipBytes(size);
    return string;
  }

  private static class RequestDecoder implements ZMTPMessageDecoder<Request> {

    enum State {
      URI,
      METHOD,
      TIMESTAMP,
      PAYLOAD
    }

    private State state = State.URI;

    private String uri;
    private String method;
    private long timestamp;
    private ByteBuffer payload;

    @Override
    public void readFrame(final ByteBuf data, final int size, final boolean more) {
      switch (state) {
        case URI:
          uri = readString(data, size);
          state = State.METHOD;
          break;
        case METHOD:
          method = readString(data, size);
          state = State.TIMESTAMP;
          break;
        case TIMESTAMP:
          timestamp = readLong(data, size);
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          payload = readByteBuffer(data, size);
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
          state = State.TIMESTAMP;
          break;
        case TIMESTAMP:
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          state = State.URI;
          break;
      }
    }

    @Override
    public Request finish(final boolean truncated) {
      return new Request(uri, method, timestamp, payload);
    }
  }

  private static class ReplyDecoder implements ZMTPMessageDecoder<Reply> {

    enum State {
      URI,
      METHOD,
      STATUSCODE,
      TIMESTAMP,
      PAYLOAD
    }

    private State state = State.URI;

    private String uri;
    private String method;
    private int statusCode;
    private long timestamp;
    private ByteBuffer payload;

    @Override
    public void readFrame(final ByteBuf data, final int size, final boolean more) {
      switch (state) {
        case URI:
          uri = readString(data, size);
          state = State.METHOD;
          break;
        case METHOD:
          method = readString(data, size);
          state = State.STATUSCODE;
          break;
        case STATUSCODE:
          statusCode = readInt(data, size);
          state = State.TIMESTAMP;
          break;
        case TIMESTAMP:
          timestamp = readLong(data, size);
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          payload = readByteBuffer(data, size);
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
          state = State.TIMESTAMP;
          break;
        case TIMESTAMP:
          state = State.PAYLOAD;
          break;
        case PAYLOAD:
          state = State.URI;
          break;
      }
    }

    @Override
    public Reply finish(final boolean truncated) {
      return new Reply(uri, method, statusCode, timestamp, payload);
    }
  }
}
