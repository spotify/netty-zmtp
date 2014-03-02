package com.spotify.netty.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.spotify.netty.handler.codec.zmtp.TestUtil.buf;
import static com.spotify.netty.handler.codec.zmtp.TestUtil.cmp;

/**
 * These tests has a full pipeline setup.
 */
public class PipelineTests {

    private EmbeddedChannel channel;

    @Test
    public void testZMTPPipeline() {
        ZMTPSession s = new ZMTPSession(
                ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
        channel = new EmbeddedChannel(new ZMTP20Codec(s, true));

        cmp(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), (ByteBuf) channel.readOutbound());
        channel.writeInbound(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63));
        cmp(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), (ByteBuf) channel.readOutbound());

        channel.writeInbound(buf(1, 1, 0x65, 1, 0, 0, 1, 0x62));
        ZMTPIncomingMessage m = (ZMTPIncomingMessage) channel.readInbound();

        List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
        Assert.assertEquals(1, envelope.size());
        Assert.assertArrayEquals(new byte[]{0x65}, envelope.get(0).getData());

        List<ZMTPFrame> body = m.getMessage().getContent();
        Assert.assertEquals(1, body.size());
        Assert.assertArrayEquals(new byte[]{0x62}, body.get(0).getData());
        Assert.assertFalse(channel.finish());
    }

    @Test
    public void testZMTPPipelineFragmented() {
        ZMTPSession s = new ZMTPSession(
                ZMTPConnectionType.Addressed, 1024, "foo".getBytes(), ZMTPSocketType.REQ);
        channel = new EmbeddedChannel(new ZMTP20Codec(s, true));

        cmp(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 4, 0x7f), (ByteBuf) channel.readOutbound());
        channel.writeInbound(buf(0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 1, 4, 0, 1, 0x63, 1, 1, 0x65, 1));
        cmp(buf(1, 3, 0, 3, 0x66, 0x6f, 0x6f), (ByteBuf) channel.readOutbound());

        channel.writeInbound(buf(0, 0, 1, 0x62));
        ZMTPIncomingMessage m = (ZMTPIncomingMessage) channel.readInbound();

        List<ZMTPFrame> envelope = m.getMessage().getEnvelope();
        Assert.assertEquals(1, envelope.size());
        Assert.assertArrayEquals(new byte[]{0x65}, envelope.get(0).getData());

        List<ZMTPFrame> body = m.getMessage().getContent();
        Assert.assertEquals(1, body.size());
        Assert.assertArrayEquals(new byte[]{0x62}, body.get(0).getData());
        Assert.assertFalse(channel.finish());
    }
}
