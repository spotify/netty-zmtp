package com.spotify.netty.handler.codec.zmtp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.buf;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;

/**
 * These tests has a full pipeline setup.
 */
public class PipelineTests {

  byte[] LONG_MSG = ("To use netty-zmtp, insert one of `ZMTP10Codec` or `ZMTP20Codec` into your " +
      "`ChannelPipeline` and it will turn incoming buffers into  `ZMTPIncomingMessage` " +
      "instances up the pipeline and accept `ZMTPMessage` instances that gets serialized into " +
      "buffers downstream.").getBytes();

  /**
   * First let's just exercise the PipelineTester a bit.
   */
  @Test
  public void testPipelineTester() {
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer("Hello, world".getBytes());
    ChannelPipeline pipeline = Channels.pipeline(new SimpleChannelUpstreamHandler() {

      @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
          throws Exception {
        e.getChannel().write(buf);
        ctx.sendUpstream(e);
      }
    });
    PipelineTester pipelineTester = new PipelineTester(pipeline);
    Assert.assertEquals(buf, pipelineTester.readClient());

    ChannelBuffer another = ChannelBuffers.wrappedBuffer("foo".getBytes());
    pipelineTester.writeClient(TestUtil.clone(another));

    cmp(another,(ChannelBuffer) pipelineTester.readServer());

    another = ChannelBuffers.wrappedBuffer("bar".getBytes());
    pipelineTester.writeServer(TestUtil.clone(another));
    cmp(another,pipelineTester.readClient());
  }

  @Test
  public void testZMTPPipeline() {
    ZMTPSession s = new ZMTPSession(
        ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
    ChannelPipeline p = Channels.pipeline(new ZMTP20Codec(s, true));

    PipelineTester pt = new PipelineTester(p);
    cmp(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), pt.readClient());
    pt.writeClient(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63));
    cmp(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), pt.readClient());

    pt.writeClient(buf(1, 1, 0x65, 1, 0, 0, 1, 0x62));
    ZMTPIncomingMessage m = (ZMTPIncomingMessage)pt.readServer();

    List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(1, envelope.size());
    cmp(buf(0x65), envelope.get(0).getDataBuffer());

    List<ZMTPFrame> body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(0x62), body.get(0).getDataBuffer());


  }

  @Test
  public void testZMTPPipelineFragmented() {
    ZMTPSession s = new ZMTPSession(
        ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
    ChannelPipeline p = Channels.pipeline(new ZMTP20Codec(s, true));

    PipelineTester pt = new PipelineTester(p);
    cmp(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), pt.readClient());
    pt.writeClient(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63, 1, 1, 0x65, 1));
    cmp(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), pt.readClient());

    pt.writeClient(buf(0, 0, 1, 0x62));
    ZMTPIncomingMessage m = (ZMTPIncomingMessage)pt.readServer();

    List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(1, envelope.size());
    cmp(buf(0x65), envelope.get(0).getDataBuffer());

    List<ZMTPFrame> body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(0x62), body.get(0).getDataBuffer());


  }

  @Test
  public void testZMTP1PipelineLongMessage() {
    ZMTPSession s = new ZMTPSession(
        ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
    ChannelPipeline p = Channels.pipeline(new ZMTP10Codec(s));

    PipelineTester pt = new PipelineTester(p);
    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // content frame size + flag octet
    cb.writeBytes(bytes(0x0ff, 0, 0, 0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);
    ZMTPIncomingMessage m = (ZMTPIncomingMessage)pt.readServer();

    List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(0, envelope.size());

    List<ZMTPFrame> body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(LONG_MSG), body.get(0).getDataBuffer());
  }

  @Test
  // tests the case when the message to be parsed is fragmented inside the long long size field
  public void testZMTP1PipelineLongMessageFragmentedLong() {
    ZMTPSession s = new ZMTPSession(
        ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
    ChannelPipeline p = Channels.pipeline(new ZMTP10Codec(s));

    PipelineTester pt = new PipelineTester(p);
    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // fragmented first part of frame size
    cb.writeBytes(bytes(0x0ff, 0, 0));

    pt.writeClient(cb);

    cb = ChannelBuffers.dynamicBuffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);

    ZMTPIncomingMessage m = (ZMTPIncomingMessage)pt.readServer();

    List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(0, envelope.size());

    List<ZMTPFrame> body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(LONG_MSG), body.get(0).getDataBuffer());
  }

  @Test
  // tests the case when the message to be parsed is fragmented between 0xff flag and 8 octet length
  public void testZMTP1PipelineLongMessageFragmentedSize() {
    ZMTPSession s = new ZMTPSession(
        ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
    ChannelPipeline p = Channels.pipeline(new ZMTP10Codec(s));

    PipelineTester pt = new PipelineTester(p);
    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // fragmented first part of frame size
    cb.writeBytes(bytes(0x0ff));

    pt.writeClient(cb);

    cb = ChannelBuffers.dynamicBuffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);

    ZMTPIncomingMessage m = (ZMTPIncomingMessage)pt.readServer();

    List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(0, envelope.size());

    List<ZMTPFrame> body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(LONG_MSG), body.get(0).getDataBuffer());
  }


  @Test
  // tests fragmentation in the size field of the second message
  public void testZMTP1PipelineMultiMessage() {
    ZMTPSession s = new ZMTPSession(
        ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
    ChannelPipeline p = Channels.pipeline(new ZMTP10Codec(s));

    PipelineTester pt = new PipelineTester(p);
    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // content frame size + flag octet
    cb.writeBytes(bytes(0x0ff, 0, 0, 0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);
    // second message, first fragment
    // fragmented first part of frame size
    cb.writeBytes(bytes(1, 1, 0x0ff, 0, 0));


    pt.writeClient(cb);
    ZMTPIncomingMessage m = (ZMTPIncomingMessage)pt.readServer();

    List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(0, envelope.size());

    List<ZMTPFrame> body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(LONG_MSG), body.get(0).getDataBuffer());

    // send the rest of the second message
    cb = ChannelBuffers.dynamicBuffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);
    pt.writeClient(cb);

    m = (ZMTPIncomingMessage)pt.readServer();

    envelope = m.getMessage().getEnvelope();
    Assert.assertEquals(0, envelope.size());

    body = m.getMessage().getContent();
    Assert.assertEquals(1, body.size());
    cmp(buf(LONG_MSG), body.get(0).getDataBuffer());

  }

}
