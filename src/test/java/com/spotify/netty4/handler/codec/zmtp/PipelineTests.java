package com.spotify.netty4.handler.codec.zmtp;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static com.spotify.netty4.handler.codec.zmtp.TestUtil.buf;
import static com.spotify.netty4.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty4.handler.codec.zmtp.TestUtil.cmp;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.REQ;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.junit.Assert.assertEquals;

/**
 * These tests has a full pipeline setup.
 */
public class PipelineTests {

  private final byte[] LONG_MSG = (
      "To use netty-zmtp, insert one of `ZMTP10Codec` or `ZMTP20Codec` into your " +
      "`ChannelPipeline` and it will turn incoming buffers into  `ZMTPIncomingMessage` " +
      "instances up the pipeline and accept `ZMTPMessage` instances that gets serialized into " +
      "buffers downstream.").getBytes();

  /**
   * First let's just exercise the PipelineTester a bit.
   */
  @Test
  public void testPipelineTester() {
    final ByteBuf buf = Unpooled.wrappedBuffer("Hello, world".getBytes());

    final PipelineTester pipelineTester = new PipelineTester(new ChannelInboundHandlerAdapter() {

      @Override
      public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ctx.channel().writeAndFlush(buf);
      }
    });
    assertEquals(buf, pipelineTester.readClient());

    ByteBuf foo = Unpooled.copiedBuffer("foo", UTF_8);
    pipelineTester.writeClient(TestUtil.clone(foo));

    cmp(foo, (ByteBuf) pipelineTester.readServer());

    ByteBuf bar = Unpooled.copiedBuffer("bar", UTF_8);
    pipelineTester.writeServer(TestUtil.clone(bar));
    cmp(bar, pipelineTester.readClient());
  }

  @Test
  public void testZMTPPipeline() {

    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP20)
            .socketType(REQ)
            .localIdentity("foo")
            .build());
    cmp(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), pt.readClient());
    pt.writeClient(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63));
    cmp(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), pt.readClient());

    pt.writeClient(buf(1, 1, 0x65, 1, 0, 0, 1, 0x62));
    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(3, m.message().size());
    cmp(buf(0x65), m.message().frame(0));
    cmp(buf(), m.message().frame(1));
    cmp(buf(0x62), m.message().frame(2));
  }

  @Test
  public void testZMTPPipelineFragmented() {
    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP20)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    cmp(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), pt.readClient());
    pt.writeClient(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63, 1, 1, 0x65, 1));
    cmp(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), pt.readClient());

    pt.writeClient(buf(0, 0, 1, 0x62));
    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(3, m.message().size());
    cmp(buf(0x65), m.message().frame(0));
    cmp(buf(), m.message().frame(1));
    cmp(buf(0x62), m.message().frame(2));
  }

  @Test
  public void testZMTP1PipelineLongMessage() {
    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ByteBuf cb = Unpooled.buffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // content frame size + flag octet
    cb.writeBytes(bytes(0x0ff, 0, 0, 0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);
    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(2, m.message().size());
    cmp(buf(), m.message().frame(0));
    cmp(buf(LONG_MSG), m.message().frame(1));
  }

  @Test
  public void testZMTP1PipelineFragmentedHandshake() {
    doTestZMTP1PipelineFragmentedHandshake(buf(4), buf(0, 0x62, 0x61, 0x72));
    doTestZMTP1PipelineFragmentedHandshake(buf(4, 0), buf(0x62, 0x61, 0x72));
    doTestZMTP1PipelineFragmentedHandshake(buf(4, 0, 0x62), buf(0x61, 0x72));
    doTestZMTP1PipelineFragmentedHandshake(buf(4, 0, 0x62, 0x61), buf(0x72));
  }

  private void doTestZMTP1PipelineFragmentedHandshake(ByteBuf first, ByteBuf second) {
    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    // write both fragments of client handshake
    pt.writeClient(first);
    pt.writeClient(second);

    ByteBuf cb = Unpooled.buffer();
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // content frame size + flag octet
    cb.writeBytes(bytes(0x0ff, 0, 0, 0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);
    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(2, m.message().size());
    cmp(buf(), m.message().frame(0));
    cmp(buf(LONG_MSG), m.message().frame(1));
  }


  @Test
  // tests the case when the message to be parsed is fragmented inside the long long size field
  public void testZMTP1PipelineLongMessageFragmentedLong() {
    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ByteBuf cb = Unpooled.buffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // fragmented first part of frame size
    cb.writeBytes(bytes(0x0ff, 0, 0));

    pt.writeClient(cb);

    cb = Unpooled.buffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);

    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(2, m.message().size());
    cmp(buf(), m.message().frame(0));
    cmp(buf(LONG_MSG), m.message().frame(1));
  }

  @Test
  // tests the case when the message to be parsed is fragmented between 0xff flag and 8 octet length
  public void testZMTP1PipelineLongMessageFragmentedSize() {
    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ByteBuf cb = Unpooled.buffer();
    // handshake: length + flag + client identity octets "BAR"
    cb.writeBytes(buf(4, 0, 0x62, 0x61, 0x72));
    // two octet envelope delimiter
    cb.writeBytes(bytes(0x01, 0x01));
    // fragmented first part of frame size
    cb.writeBytes(bytes(0x0ff));

    pt.writeClient(cb);

    cb = Unpooled.buffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);

    pt.writeClient(cb);

    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(2, m.message().size());
    cmp(buf(), m.message().frame(0));
    cmp(buf(LONG_MSG), m.message().frame(1));
  }


  @Test
  // tests fragmentation in the size field of the second message
  public void testZMTP1PipelineMultiMessage() {
    PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    cmp(buf(0x04, 0, 0x66, 0x6f, 0x6f), pt.readClient());

    ByteBuf cb = Unpooled.buffer();
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
    ZMTPIncomingMessage m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(2, m.message().size());
    cmp(buf(), m.message().frame(0));
    cmp(buf(LONG_MSG), m.message().frame(1));

    // send the rest of the second message
    cb = Unpooled.buffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);
    pt.writeClient(cb);

    m = (ZMTPIncomingMessage) pt.readServer();

    assertEquals(2, m.message().size());
    cmp(buf(), m.message().frame(0));
    cmp(buf(LONG_MSG), m.message().frame(1));
  }

}
