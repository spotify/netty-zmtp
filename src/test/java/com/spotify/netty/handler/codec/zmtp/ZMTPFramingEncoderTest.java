package com.spotify.netty.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.bytes;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static org.junit.Assert.assertFalse;

/**
 * Tests FramingEncoder
 */
public class ZMTPFramingEncoderTest {

    private static final byte[] LARGE_FILL = new byte[500];

    static {
        fill(LARGE_FILL, (byte) 0x61);
    }

    private EmbeddedChannel channel;

    @Test
    public void testEncodeZMTP1() throws Exception {

        ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
        session.setActualVersion(1);
        ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);
        channel = new EmbeddedChannel(enc);

        ZMTPMessage message = new ZMTPMessage(
                asList(ZMTPFrame.create("id0"), ZMTPFrame.create("id1")),
                asList(ZMTPFrame.create("f0")));

        channel.writeOutbound(message);
        ByteBuf buf = (ByteBuf) channel.readOutbound();
        cmp(buf, 4, 1, 0x69, 0x64, 0x30, 4, 1, 0x69, 0x64, 0x31, 1, 1, 3, 0, 0x66, 0x30);

        assertFalse(channel.finish());
    }

    @Test
    public void testEncodeZMTP2() throws Exception {
        ZMTPMessage message = new ZMTPMessage(
                asList(ZMTPFrame.create("id0"), ZMTPFrame.create("id1")),
                asList(ZMTPFrame.create("f0")));

        ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
        session.setActualVersion(2);
        ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);
        channel = new EmbeddedChannel(enc);

        channel.writeOutbound(message);
        ByteBuf buf = (ByteBuf) channel.readOutbound();
        cmp(buf, 1, 3, 0x69, 0x64, 0x30, 1, 3, 0x69, 0x64, 0x31, 1, 0, 0, 2, 0x66, 0x30);

        assertFalse(channel.finish());
    }

    @Test
    public void testEncodeZMTP2Long() throws Exception {
        ZMTPMessage message = new ZMTPMessage(
                asList(ZMTPFrame.create("id0")),
                asList(ZMTPFrame.create(LARGE_FILL)));
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(bytes(1, 3, 0x69, 0x64, 0x30, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0x01, 0xf4));
        buf.writeBytes(LARGE_FILL);

        ZMTPSession session = new ZMTPSession(ZMTPConnectionType.Addressed, 1024);
        session.setActualVersion(2);
        ZMTPFramingEncoder enc = new ZMTPFramingEncoder(session);
        channel = new EmbeddedChannel(enc);

        channel.writeOutbound(message);
        ByteBuf out = (ByteBuf) channel.readOutbound();
        cmp(buf, out);

        assertFalse(channel.finish());
    }
}
