package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.base.Strings;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;

import static com.spotify.netty4.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty4.handler.codec.zmtp.TestUtil.cmp;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocol.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZMTPFramingEncoderTest {

  @Mock ChannelHandlerContext ctx;
  @Mock ChannelPromise promise;
  @Mock EventExecutor executor;

  @Captor ArgumentCaptor<ByteBuf> bufCaptor;

  private static final String LARGE_FILL = Strings.repeat("a", 500);

  @Before
  public void setUp() {
    when(ctx.write(bufCaptor.capture(), any(ChannelPromise.class))).thenReturn(promise);
    when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
    when(ctx.executor()).thenReturn(executor);
  }

  @Test
  public void testEncodeZMTP1() throws Exception {

    ZMTPConfig config = ZMTPConfig.builder()
        .protocol(ZMTP10)
        .socketType(DEALER)
        .build();
    ZMTPSession session = new ZMTPSession(config);
    session.handshakeDone(ZMTPHandshake.of(ZMTPVersion.ZMTP10, null));

    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    ZMTPMessage message = ZMTPMessage.fromUTF8("id0", "id1", "", "f0");

    enc.write(ctx, message, promise);
    enc.flush(ctx);
    final ByteBuf buf = bufCaptor.getValue();
    cmp(buf, 4, 1, 0x69, 0x64, 0x30, 4, 1, 0x69, 0x64, 0x31, 1, 1, 3, 0, 0x66, 0x30);
    buf.release();
  }

  @Test
  public void testEncodeZMTP2() throws Exception {

    ZMTPMessage message = ZMTPMessage.fromUTF8("id0", "id1", "", "f0");

    ZMTPConfig config = ZMTPConfig.builder()
        .protocol(ZMTP20)
        .socketType(DEALER)
        .build();
    ZMTPSession session = new ZMTPSession(config);
    session.handshakeDone(ZMTPHandshake.of(ZMTPVersion.ZMTP20, null));

    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    enc.write(ctx, message, promise);
    enc.flush(ctx);
    final ByteBuf buf = bufCaptor.getValue();
    cmp(buf, 1, 3, 0x69, 0x64, 0x30, 1, 3, 0x69, 0x64, 0x31, 1, 0, 0, 2, 0x66, 0x30);
    buf.release();
  }

  @Test
  public void testEncodeZMTP2Long() throws Exception {
    ZMTPMessage message = ZMTPMessage.fromUTF8("id0", "", LARGE_FILL);
    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(bytes(1, 3, 0x69, 0x64, 0x30,
                         1, 0,
                         2, 0, 0, 0, 0, 0, 0, 0x01, 0xf4));
    buf.writeBytes(LARGE_FILL.getBytes(UTF_8));

    ZMTPConfig config = ZMTPConfig.builder()
        .protocol(ZMTP20)
        .socketType(DEALER)
        .build();
    ZMTPSession session = new ZMTPSession(config);

    session.handshakeDone(ZMTPHandshake.of(ZMTPVersion.ZMTP20, null));

    ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);

    enc.write(ctx, message, promise);
    enc.flush(ctx);
    final ByteBuf buf2 = bufCaptor.getValue();

    cmp(buf, buf2);

    buf.release();
    buf2.release();
  }
}
