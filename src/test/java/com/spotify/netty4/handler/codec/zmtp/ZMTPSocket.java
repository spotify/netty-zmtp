/*
* Copyright (c) 2012-2015 Spotify AB
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

package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.spotify.netty4.util.BatchFlusher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.spotify.netty4.handler.codec.zmtp.ListenableFutureAdapter.listenable;

/**
 * A simple ZMTP socket implementation for testing purposes.
 */
public class ZMTPSocket implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(ZMTPSocket.class);

  private static final ListeningExecutorService EXECUTOR =
      MoreExecutors.listeningDecorator(GlobalEventExecutor.INSTANCE);

  /**
   * Represents a connected peer.
   */
  public interface ZMTPPeer {

    /**
     * Get the ZMTP session for this peer.
     */
    ZMTPSession session();

    /**
     * Send a message to this peer.
     */
    ListenableFuture<Void> send(ZMTPMessage message);
  }

  /**
   * Handles incoming messages and connection events.
   */
  public interface Handler {

    /**
     * A peer connected.
     */
    void connected(ZMTPSocket socket, ZMTPPeer peer);

    /**
     * A peer disconnected.
     */
    void disconnected(ZMTPSocket socket, ZMTPPeer peer);

    /**
     * A message was received from a peer.
     */
    void message(ZMTPSocket socket, ZMTPPeer peer, ZMTPMessage message);
  }

  private interface Sender {

    ListenableFuture<Void> send(ZMTPMessage message);
  }

  private interface Receiver {

    void receive(final ZMTPPeer peer, ZMTPMessage message);
  }

  private static final ThreadFactory DAEMON =
      new ThreadFactoryBuilder().setDaemon(true).build();

  private static final NioEventLoopGroup GROUP = new NioEventLoopGroup(1, DAEMON);

  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private volatile List<ZMTPPeer> peers = ImmutableList.of();
  private volatile Map<ByteBuf, ZMTPPeer> routing = ImmutableMap.of();
  private final Object lock = new Object();

  private final Sender sender;
  private final Receiver receiver;

  private final Handler handler;
  private final ZMTPConfig config;

  private volatile boolean closed;

  /**
   * Create a new socket.
   */
  private ZMTPSocket(final Handler handler, final ZMTPConfig config) {
    this.handler = checkNotNull(handler, "handler");
    this.config = config.toBuilder()
        .identityGenerator(new IdentityGenerator())
        .decoder(decoder(config.socketType()))
        .encoder(encoder(config.socketType()))
        .build();
    this.sender = sender(config.socketType());
    this.receiver = receiver(config.socketType());
  }

  /**
   * Bind this socket to an endpoint.
   */
  public ListenableFuture<InetSocketAddress> bind(final String endpoint) {
    return transform(address(endpoint), new AsyncFunction<InetSocketAddress, InetSocketAddress>() {
      @Override
      public ListenableFuture<InetSocketAddress> apply(
          @SuppressWarnings("NullableProblems") final InetSocketAddress input)
          throws Exception {
        return bind(input);
      }
    });
  }

  /**
   * Bind this socket to an address.
   */
  public ListenableFuture<InetSocketAddress> bind(final InetSocketAddress address) {
    final ServerBootstrap b = new ServerBootstrap()
        .channel(NioServerSocketChannel.class)
        .group(GROUP)
        .childHandler(new ChannelInitializer());
    final ChannelFuture f = b.bind(address);
    channelGroup.add(f.channel());
    if (closed) {
      f.channel().close();
      return immediateFailedFuture(new ClosedChannelException());
    }
    return transform(listenable(f), new Function<Void, InetSocketAddress>() {
      @Override
      public InetSocketAddress apply(final Void input) {
        return (InetSocketAddress) f.channel().localAddress();
      }
    });
  }

  /**
   * Connect this socket to an endpoint.
   */
  public ListenableFuture<Void> connect(final String endpoint) {
    return transform(address(endpoint), new AsyncFunction<InetSocketAddress, Void>() {
      @Override
      public ListenableFuture<Void> apply(
          @SuppressWarnings("NullableProblems") final InetSocketAddress address) throws Exception {
        return connect(address);
      }
    });
  }

  /**
   * Connect this socket to an address.
   */
  public ListenableFuture<Void> connect(final InetSocketAddress address) {
    final Bootstrap b = new Bootstrap()
        .group(GROUP)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer());
    final ChannelFuture f = b.connect(address);
    if (closed) {
      f.channel().close();
      return immediateFailedFuture(new ClosedChannelException());
    }
    return listenable(f);
  }

  /**
   * Send a message on this socket.
   */
  public ListenableFuture<Void> send(final ZMTPMessage message) {
    return sender.send(message);
  }

  /**
   * Close this socket.
   */
  @Override
  public void close() {
    closed = true;
    channelGroup.close().awaitUninterruptibly();
  }

  /**
   * Get a list of all connected peers.
   */
  public List<ZMTPPeer> peers() {
    return peers;
  }

  /**
   * Get a sender for a socket type.
   */
  private ZMTPEncoder.Factory encoder(final ZMTPSocketType socketType) {
    switch (socketType) {
      case ROUTER:
        return RoutingEncoder.FACTORY;
      default:
        return ZMTPMessageEncoder.FACTORY;
    }
  }

  /**
   * Get a decoder for a socket type.
   */
  private ZMTPDecoder.Factory decoder(final ZMTPSocketType socketType) {
    switch (socketType) {
      case ROUTER:
        return RoutingDecoder.FACTORY;
      default:
        return ZMTPMessageDecoder.FACTORY;
    }
  }

  /**
   * Get a receiver that implements the appropriate socket type behavior.
   */
  private Receiver receiver(final ZMTPSocketType socketType) {
    switch (socketType) {
      case PUSH:
        return new DropReceiver();
      case DEALER:
      case ROUTER:
      case SUB:
      case PUB:
      case PULL:
        return new PassReceiver();
      default:
        throw new IllegalArgumentException("Unsupported socket type: " + socketType);
    }
  }

  /**
   * Get a sender that implements the appropriate socket type behavior.
   */
  private Sender sender(final ZMTPSocketType socketType) {
    switch (socketType) {
      case PUSH:
      case DEALER:
        return new RoundRobinSender();
      case ROUTER:
        return new RoutingSender();
      case SUB:
      case PUB:
        return new BroadcastSender();
      case PULL:
        return new UnsupportedOperationSender();
      default:
        throw new IllegalArgumentException("Unsupported socket type: " + socketType);
    }
  }

  /**
   * Resolve an endpoint into an address. Async to avoid blocking on DNS resolution.
   */
  private static ListenableFuture<InetSocketAddress> address(final String endpoint) {
    return EXECUTOR.submit(new Callable<InetSocketAddress>() {
      @Override
      public InetSocketAddress call() throws Exception {
        final URI uri = URI.create(endpoint);
        checkArgument("tcp".equals(uri.getScheme()),
                      "Unsupported endpoint type: %s", uri.getScheme());
        final List<String> parts = Splitter.on(':').splitToList(uri.getAuthority());
        final String hostString = parts.get(0);
        final InetAddress host = hostString.equals("*") ? null : InetAddress.getByName(hostString);
        final String portString = parts.get(1);
        final int port = portString.equals("*") ? 0 : Integer.valueOf(portString);
        return new InetSocketAddress(host, port);
      }
    });
  }

  /**
   * Handles a single connected peer.
   */
  private class Peer extends ChannelInboundHandlerAdapter implements ZMTPPeer {

    private final Channel ch;
    private final BatchFlusher flusher;
    private final ZMTPSession session;

    public Peer(final Channel ch, final ZMTPSession session) {
      this.ch = ch;
      this.session = session;
      this.flusher = new BatchFlusher(ch);
    }

    @Override
    public ZMTPSession session() {
      return session;
    }

    @Override
    public ListenableFuture<Void> send(final ZMTPMessage message) {
      final ChannelFuture f = ch.write(message);
      flusher.flush();
      return listenable(f);
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt)
        throws Exception {
      if (evt instanceof ZMTPHandshakeSuccess) {
        register(session.peerIdentity());
        try {
          handler.connected(ZMTPSocket.this, this);
        } catch (Exception e) {
          log.error("handler threw exception", e);
        }
      }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      deregister(session.peerIdentity());
      try {
        handler.disconnected(ZMTPSocket.this, this);
      } catch (Exception e) {
        log.error("handler threw exception", e);
      }
    }

    private void register(final ByteBuffer identity) {
      synchronized (lock) {
        peers = ImmutableList.<ZMTPPeer>builder()
            .addAll(peers)
            .add(this)
            .build();
        routing = ImmutableMap.<ByteBuf, ZMTPPeer>builder()
            .putAll(routing)
            .put(Unpooled.wrappedBuffer(identity), this)
            .build();
      }
    }

    private void deregister(final ByteBuffer identity) {
      synchronized (lock) {
        final ImmutableList.Builder<ZMTPPeer> newPeers = ImmutableList.builder();
        for (final ZMTPPeer handler : peers) {
          if (handler != this) {
            newPeers.add(handler);
          }
        }
        peers = newPeers.build();
        final ImmutableMap.Builder<ByteBuf, ZMTPPeer> newRouting = ImmutableMap.builder();
        final ByteBuf id = Unpooled.wrappedBuffer(identity);
        for (final Map.Entry<ByteBuf, ZMTPPeer> entry : routing.entrySet()) {
          if (!entry.getKey().equals(id)) { newRouting.put(entry.getKey(), entry.getValue()); }
        }
        routing = newRouting.build();
      }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg)
        throws Exception {
      if (msg instanceof ZMTPMessage) {
        try {
          receiver.receive(this, (ZMTPMessage) msg);
        } catch (Exception e) {
          log.error("handler threw exception", e);
        }
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
        throws Exception {
      log.warn("exception", cause);
      ctx.close();
    }
  }

  private class ChannelInitializer extends io.netty.channel.ChannelInitializer {

    @Override
    protected void initChannel(final Channel ch) throws Exception {
      channelGroup.add(ch);
      final ZMTPCodec codec = ZMTPCodec.from(config);
      final Peer peer = new Peer(ch, codec.session());
      ch.pipeline().addLast(codec, peer);
    }
  }

  /**
   * A sender that round robin load balances messages over all connected peers.
   */
  private class RoundRobinSender implements Sender {

    private final AtomicInteger i = new AtomicInteger();

    @Override
    public ListenableFuture<Void> send(final ZMTPMessage message) {
      final List<ZMTPPeer> channels = peers();
      if (channels.size() == 0) {
        return immediateFailedFuture(new ClosedChannelException());
      }
      final ZMTPPeer handler = next(channels);
      return handler.send(message);
    }

    private ZMTPPeer next(final List<ZMTPPeer> channels) {
      assert !channels.isEmpty();
      int next;
      int prev;
      do {
        prev = i.get();
        next = prev + 1;
        if (next >= channels.size()) {
          next = 0;
        }
      } while (!i.compareAndSet(prev, next));
      return channels.get(next);
    }
  }

  /**
   * A sender that routes message to the appropriate peer using the front identity frame.
   */
  private class RoutingSender implements Sender {

    @Override
    public ListenableFuture<Void> send(final ZMTPMessage message) {
      if (message.size() == 0) {
        return immediateFailedFuture(new IllegalArgumentException("empty message"));
      }
      final ByteBuf identity = message.frame(0);
      final ZMTPPeer peer = routing.get(identity);
      if (peer == null) {
        message.release();
        return immediateFailedFuture(new ClosedChannelException());
      }
      return peer.send(message);
    }
  }

  /**
   * A sender that broadcasts messages to all connected peers.
   */
  private class BroadcastSender implements Sender {

    @Override
    public ListenableFuture<Void> send(final ZMTPMessage message) {
      final List<ZMTPPeer> channels = peers();
      for (final ZMTPPeer handler : channels) {
        handler.send(message);
      }
      return immediateFuture(null);
    }
  }

  /**
   * A sender that immediately fails all send operations.
   */
  private class UnsupportedOperationSender implements Sender {

    @Override
    public ListenableFuture<Void> send(final ZMTPMessage message) {
      return immediateFailedFuture(new UnsupportedOperationException());
    }
  }

  /**
   * A receiver that drops all incoming messages.
   */
  private class DropReceiver implements Receiver {

    @Override
    public void receive(final ZMTPPeer peer, final ZMTPMessage message) {
      message.release();
    }
  }

  /**
   * A receive that passes on all incoming messages to the {@link Handler}.
   */
  private class PassReceiver implements Receiver {

    @Override
    public void receive(final ZMTPPeer peer, final ZMTPMessage message) {
      try {
        handler.message(ZMTPSocket.this, peer, message);
      } catch (Exception e) {
        log.error("handler threw exception", e);
      }
    }
  }

  /**
   * A {@link ZMTPMessage} decoder that pushes the peer identity onto the front of the message.
   */
  private static class RoutingDecoder implements ZMTPDecoder {

    private static final ZMTPDecoder.Factory FACTORY = new Factory() {
      @Override
      public ZMTPDecoder decoder(final ZMTPSession session) {
        return new RoutingDecoder(session.peerIdentity());
      }
    };

    private final ByteBuf DELIMITER = Unpooled.EMPTY_BUFFER;

    private final ByteBuffer identity;

    private final List<ByteBuf> frames = new ArrayList<ByteBuf>();
    private int frameLength;

    RoutingDecoder(final ByteBuffer identity) {
      this.identity = identity;
      reset();
    }

    private void reset() {
      frames.clear();
      frames.add(Unpooled.wrappedBuffer(identity));
      frameLength = 0;
    }

    @Override
    public void header(final ChannelHandlerContext ctx, final long length, final boolean more,
                       final List<Object> out) {
      frameLength = (int) length;
    }

    @Override
    public void content(final ChannelHandlerContext ctx, final ByteBuf data,
                        final List<Object> out) {
      if (data.readableBytes() < frameLength) {
        return;
      }
      if (frameLength == 0) {
        frames.add(DELIMITER);
        return;
      }
      final ByteBuf frame = data.readSlice(frameLength);
      frame.retain();
      frames.add(frame);
    }

    @Override
    public void finish(final ChannelHandlerContext ctx, final List<Object> out) {
      final ZMTPMessage message = ZMTPMessage.from(frames);
      reset();
      out.add(message);
    }

    @Override
    public void close() {
      for (final ByteBuf frame : frames) {
        frame.release();
      }
      frames.clear();
    }
  }

  /**
   * A {@link ZMTPMessage} encoder that pops the peer identity from the front of the message.
   */
  private static class RoutingEncoder implements ZMTPEncoder {

    private static final ZMTPEncoder.Factory FACTORY = new Factory() {
      @Override
      public ZMTPEncoder encoder(final ZMTPSession session) {
        return new RoutingEncoder();
      }
    };

    @Override
    public void estimate(final Object msg, final ZMTPEstimator estimator) {
      final ZMTPMessage message = (ZMTPMessage) msg;
      for (int i = 1; i < message.size(); i++) {
        final ByteBuf frame = message.frame(i);
        estimator.frame(frame.readableBytes());
      }
    }

    @Override
    public void encode(final Object msg, final ZMTPWriter writer) {
      final ZMTPMessage message = (ZMTPMessage) msg;
      for (int i = 1; i < message.size(); i++) {
        final ByteBuf frame = message.frame(i);
        final boolean more = i < message.size() - 1;
        final ByteBuf dst = writer.frame(frame.readableBytes(), more);
        dst.writeBytes(frame, frame.readerIndex(), frame.readableBytes());
      }
    }

    @Override
    public void close() {
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final ZMTPConfig.Builder config = ZMTPConfig.builder();
    private Handler handler;

    public Builder handler(final Handler handler) {
      this.handler = handler;
      return this;
    }

    public Builder protocol(final ZMTPProtocol protocol) {
      config.protocol(protocol);
      return this;
    }

    public Builder interop(final boolean interop) {
      config.interop(interop);
      return this;
    }

    public Builder type(final ZMTPSocketType socketType) {
      config.socketType(socketType);
      return this;
    }

    public Builder identity(final CharSequence identity) {
      config.localIdentity(identity);
      return this;
    }

    public Builder identity(final byte[] identity) {
      config.localIdentity(identity);
      return this;
    }

    public Builder identity(final ByteBuffer localIdentity) {
      config.localIdentity(localIdentity);
      return this;
    }

    public ZMTPSocket build() {
      return new ZMTPSocket(handler, config.build());
    }
  }

  /**
   * An identity generator that keeps an integer counter per {@link ZMTPSocket}.
   */
  private static class IdentityGenerator implements ZMTPIdentityGenerator {

    private final AtomicInteger peerIdCounter = new AtomicInteger(new SecureRandom().nextInt());

    @Override
    public ByteBuffer generateIdentity(final ZMTPSession session) {
      final ByteBuffer generated = ByteBuffer.allocate(5);
      generated.put((byte) 0);
      generated.putInt(peerIdCounter.incrementAndGet());
      generated.flip();
      return generated;
    }
  }
}
