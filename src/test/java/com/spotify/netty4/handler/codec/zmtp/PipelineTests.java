package com.spotify.netty4.handler.codec.zmtp;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static com.google.common.base.Strings.repeat;
import static com.spotify.netty4.handler.codec.zmtp.Buffers.buf;
import static com.spotify.netty4.handler.codec.zmtp.Buffers.bytes;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.REQ;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * These tests has a full pipeline setup.
 */
public class PipelineTests {

  // FIXME (dano): Tests are hardcoded to use a message 260 bytes long
  private static final byte[] LONG_MSG = repeat("data", 100).substring(0, 260).getBytes(UTF_8);

  /**
   * First let's just exercise the PipelineTester a bit.
   */
  @Test
  public void testPipelineTester() {
    final ByteBuf buf = Unpooled.copiedBuffer("Hello, world", UTF_8);

    final PipelineTester pipelineTester = new PipelineTester(new ChannelInboundHandlerAdapter() {

      @Override
      public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ctx.channel().writeAndFlush(buf);
      }
    });
    assertThat(pipelineTester.readClient(), is(buf));

    final ByteBuf foo = Unpooled.copiedBuffer("foo", UTF_8);
    pipelineTester.writeClient(foo.retain());

    assertThat(foo, is(pipelineTester.readServer()));

    final ByteBuf bar = Unpooled.copiedBuffer("bar", UTF_8);
    pipelineTester.writeServer(bar.retain());
    assertThat(bar, is(pipelineTester.readClient()));
  }

  @Test
  public void testZMTPPipeline() {

    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP20)
            .socketType(REQ)
            .localIdentity("foo")
            .build());
    assertThat(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), is(pt.readClient()));
    pt.writeClient(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63));
    assertThat(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), is(pt.readClient()));

    pt.writeClient(buf(1, 1, 0x65, 1, 0, 0, 1, 0x62));
    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(3));
    assertThat(buf(0x65), is(m.frame(0)));
    assertThat(buf(), is(m.frame(1)));
    assertThat(buf(0x62), is(m.frame(2)));
  }

  @Test
  public void testZMTPPipelineFragmented() {
    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP20)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    assertThat(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), is(pt.readClient()));
    pt.writeClient(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63, 1, 1, 0x65, 1));
    assertThat(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), is(pt.readClient()));

    pt.writeClient(buf(0, 0, 1, 0x62));
    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(3));
    assertThat(buf(0x65), is(m.frame(0)));
    assertThat(buf(), is(m.frame(1)));
    assertThat(buf(0x62), is(m.frame(2)));
  }

  @Test
  public void testZMTP1PipelineLongMessage() {
    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    assertThat(buf(0x04, 0, 0x66, 0x6f, 0x6f), is(pt.readClient()));

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
    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(2));
    assertThat(buf(), is(m.frame(0)));
    assertThat(buf(LONG_MSG), is(m.frame(1)));
  }

  @Test
  public void testZMTP1PipelineFragmentedHandshake() {
    doTestZMTP1PipelineFragmentedHandshake(buf(4), buf(0, 0x62, 0x61, 0x72));
    doTestZMTP1PipelineFragmentedHandshake(buf(4, 0), buf(0x62, 0x61, 0x72));
    doTestZMTP1PipelineFragmentedHandshake(buf(4, 0, 0x62), buf(0x61, 0x72));
    doTestZMTP1PipelineFragmentedHandshake(buf(4, 0, 0x62, 0x61), buf(0x72));
  }

  private void doTestZMTP1PipelineFragmentedHandshake(ByteBuf first, ByteBuf second) {
    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    assertThat(buf(0x04, 0, 0x66, 0x6f, 0x6f), is(pt.readClient()));

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
    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(2));
    assertThat(buf(), is(m.frame(0)));
    assertThat(buf(LONG_MSG), is(m.frame(1)));
  }


  @Test
  // tests the case when the message to be parsed is fragmented inside the long long size field
  public void testZMTP1PipelineLongMessageFragmentedLong() {
    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    assertThat(buf(0x04, 0, 0x66, 0x6f, 0x6f), is(pt.readClient()));

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

    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(2));
    assertThat(buf(), is(m.frame(0)));
    assertThat(buf(LONG_MSG), is(m.frame(1)));
  }

  @Test
  // tests the case when the message to be parsed is fragmented between 0xff flag and 8 octet length
  public void testZMTP1PipelineLongMessageFragmentedSize() {
    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    assertThat(buf(0x04, 0, 0x66, 0x6f, 0x6f), is(pt.readClient()));

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

    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(2));
    assertThat(buf(), is(m.frame(0)));
    assertThat(buf(LONG_MSG), is(m.frame(1)));
  }


  @Test
  // tests fragmentation in the size field of the second message
  public void testZMTP1PipelineMultiMessage() {
    final PipelineTester pt = new PipelineTester(
        ZMTPCodec.builder()
            .protocol(ZMTP10)
            .socketType(REQ)
            .localIdentity("foo")
            .build());

    assertThat(buf(0x04, 0, 0x66, 0x6f, 0x6f), is(pt.readClient()));

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
    ZMTPMessage m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(2));
    assertThat(buf(), is(m.frame(0)));
    assertThat(buf(LONG_MSG), is(m.frame(1)));

    // send the rest of the second message
    cb = Unpooled.buffer();
    // fragmented second part of frame size
    cb.writeBytes(bytes(0, 0, 0, 0, 0x01, 0x05, 0));
    // payload
    cb.writeBytes(LONG_MSG);
    pt.writeClient(cb);

    m = (ZMTPMessage) pt.readServer();

    assertThat(m.size(), is(2));
    assertThat(buf(), is(m.frame(0)));
    assertThat(buf(LONG_MSG), is(m.frame(1)));
  }

}
