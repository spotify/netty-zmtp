package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests the behaviours of ChannelPipelines, using the local transport method.
 * A PipelineTester instance has two ends named server and client where the server is the
 * position furthest upstream in the pipeline, and client is outside of the pipeline reading from
 * and writing to it.
 */
class PipelineTester {
  private BlockingQueue<ChannelBuffer> emittedOutside = new LinkedBlockingQueue<ChannelBuffer>();
  private BlockingQueue<Object> emittedInside = new LinkedBlockingQueue<Object>();
  private Channel outerChannel = null;
  private Channel innerChannel = null;

  /**
   * Constructs a server using pipeline and a client communicating with it.
   *
   * @param pipeline a ChannelPipeline to put in the server.
   */
  public PipelineTester(ChannelPipeline pipeline) {
    LocalAddress address = new LocalAddress(LocalAddress.EPHEMERAL);

    ServerBootstrap sb = new ServerBootstrap(new DefaultLocalServerChannelFactory());

    pipeline.addLast("pipelineTesterEndpoint", new SimpleChannelHandler() {
      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        emittedInside.put(e.getMessage());
      }

      @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
          throws Exception {
        super.channelConnected(ctx, e);
        innerChannel = e.getChannel();
      }
    });

    sb.setPipeline(pipeline);

    sb.bind(address);
    ClientBootstrap cb = new ClientBootstrap(new DefaultLocalClientChannelFactory());
    cb.getPipeline().addLast("1", new SimpleChannelHandler() {
      @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
          throws Exception {
        super.channelConnected(ctx, e);
        outerChannel = e.getChannel();
      }

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        emittedOutside.put((ChannelBuffer) e.getMessage());
      }
    });
    cb.connect(address).awaitUninterruptibly();

  }

  /**
   * Read the ChannelBuffers emitted from this pipeline in the client end.
   *
   * @return a ChannelBuffer from the the client FIFO
   */
  public ChannelBuffer readClient() {
    try {
      return emittedOutside.take();
    } catch (InterruptedException e) {
      throw new Error(e);
    }
  }

  /**
   * Write a ChannelBuffer to the pipeline from the client end.
   *
   * @param buf the ChannelBuffer to write
   */
  public void writeClient(ChannelBuffer buf) {
    outerChannel.write(buf);
  }

  /**
   * Read an Object from the server end of the pipeline. This can be a ChannelBuffer, or, if
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
    innerChannel.write(message);
  }
}
