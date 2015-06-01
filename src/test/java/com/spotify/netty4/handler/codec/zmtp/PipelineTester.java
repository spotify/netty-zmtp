package com.spotify.netty4.handler.codec.zmtp;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.ReferenceCountUtil;

/**
 * Tests the behaviours of ChannelPipelines, using the local transport method.
 * A PipelineTester instance has two ends named server and client where the server is the
 * position furthest upstream in the pipeline, and client is outside of the pipeline reading from
 * and writing to it.
 */
class PipelineTester {
  private final BlockingQueue<ByteBuf> emittedOutside = new LinkedBlockingQueue<ByteBuf>();
  private final BlockingQueue<Object> emittedInside = new LinkedBlockingQueue<Object>();
  private Channel outerChannel = null;
  private Channel innerChannel = null;

  private static final AtomicInteger port = new AtomicInteger();

  /**
   * Constructs a server using pipeline and a client communicating with it.
   *
   * @param handlers Server channel handlers.
   */
  public PipelineTester(final ChannelHandler... handlers) {
    final LocalAddress address = new LocalAddress("pipeline-tester-" + port.incrementAndGet());

    final ServerBootstrap sb = new ServerBootstrap();
    sb.group(new LocalEventLoopGroup(1), new LocalEventLoopGroup());
    sb.channel(LocalServerChannel.class);
    sb.childHandler(new ChannelInitializer<LocalChannel>() {
      @Override
      protected void initChannel(final LocalChannel ch) throws Exception {
        ch.pipeline().addLast(handlers);
        ch.pipeline().addLast("pipelineTesterEndpoint", new ChannelInboundHandlerAdapter() {

          @Override
          public void channelRead(final ChannelHandlerContext ctx, final Object msg)
              throws Exception {
            ReferenceCountUtil.releaseLater(msg);
            emittedInside.put(msg);
          }

          @Override
          public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            innerChannel = ctx.channel();
          }
        });
      }
    });
    sb.bind(address).awaitUninterruptibly();

    final Bootstrap cb = new Bootstrap();
    cb.group(new LocalEventLoopGroup());
    cb.channel(LocalChannel.class);
    cb.handler(new ChannelInitializer<LocalChannel>() {
      @Override
      protected void initChannel(final LocalChannel ch) throws Exception {
        ch.pipeline().addLast("1", new ChannelInboundHandlerAdapter() {
          @Override
          public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            outerChannel = ctx.channel();
          }

          @Override
          public void channelRead(final ChannelHandlerContext ctx, final Object msg)
              throws Exception {
            ReferenceCountUtil.releaseLater(msg);
            emittedOutside.put((ByteBuf) msg);
          }
        });
      }
    });

    cb.connect(address).awaitUninterruptibly();
  }

  /**
   * Read the ChannelBuffers emitted from this pipeline in the client end.
   *
   * @return a ByteBuf from the the client FIFO
   */
  public ByteBuf readClient() {
    try {
      return emittedOutside.take();
    } catch (InterruptedException e) {
      throw new Error(e);
    }
  }

  /**
   * Write a ByteBuf to the pipeline from the client end.
   *
   * @param buf the ByteBuf to write
   */
  public void writeClient(ByteBuf buf) {
    outerChannel.writeAndFlush(buf);
  }

  /**
   * Read an Object from the server end of the pipeline. This can be a ByteBuf, or, if
   * there is a FrameDecoder some sort of pojo.
   *
   * @return an Object read from the server end.
   */
  public Object readServer() {
    try {
      return emittedInside.take();
    } catch (InterruptedException e) {
      throw new Error(e);
    }
  }

  /**
   * Write a Object to the server end of the pipeline.
   *
   * @param message the Object to be written
   */
  public void writeServer(Object message) {
    innerChannel.writeAndFlush(message);
  }
}
