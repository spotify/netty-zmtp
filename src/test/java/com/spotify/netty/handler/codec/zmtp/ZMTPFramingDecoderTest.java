package com.spotify.netty.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * Tests ZMTPFramingDecoder
 */
public class ZMTPFramingDecoderTest {

    private EmbeddedChannel channel;

    @Test
    public void testHandshakeWithIdentity() throws Exception {
        doHandshake("something".getBytes(), "another_thing".getBytes());
    }

    @Test
    public void testHandshakeWithoutIdentity() throws Exception {
        doHandshake(null, null);
    }

    @Test
    public void testDecodeFrame() throws Exception {
        byte[] serverIdentity = "server".getBytes();
        byte[] clientIdentity = "client".getBytes();
        ZMTPFramingDecoder zfd = doHandshake(serverIdentity, clientIdentity);

        // for now, broadcast and neutral type seems to be buggy. Going with addressed
        ByteBuf cb = Unpooled.buffer();
        // header
        byte[] header = "head".getBytes();
        cb.writeByte(header.length + 1);
        cb.writeByte(0x01);
        cb.writeBytes(header);
        // delimiter
        cb.writeByte(0x01);
        cb.writeByte(0x01);
        // body
        byte[] body = "body".getBytes();
        cb.writeByte(body.length + 1);
        cb.writeByte(0x00);
        cb.writeBytes(body);
        cb.retain();

        channel.writeInbound(cb);
        Object msg = channel.readInbound();

        Assert.assertTrue(msg instanceof ZMTPIncomingMessage);

        ZMTPIncomingMessage zim = (ZMTPIncomingMessage) msg;
        ZMTPSession s = zim.getSession();

        Assert.assertArrayEquals(clientIdentity, s.getRemoteIdentity());
        Assert.assertArrayEquals(serverIdentity, s.getLocalIdentity());
        List<ZMTPFrame> frames = zim.getMessage().getContent();

        Assert.assertEquals(1, frames.size());
        Assert.assertArrayEquals("body".getBytes(), frames.get(0).getData());
    }

    @Test
    public void testTruncatedClientIdentity() throws Exception {
        byte[] serverIdentity = "third_thing".getBytes();

        ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdentity);
        ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

        // Someone connects
        channel = new EmbeddedChannel(zfd);

        Object msg = channel.readOutbound();
        Assert.assertTrue(msg instanceof ByteBuf);
        Assert.assertArrayEquals(
                ((ByteBuf) msg).array(),
                makeFrame(serverIdentity).array());

        Assert.assertNull(channel.readOutbound());

        channel.writeInbound(Unpooled.buffer(0));
        Assert.assertNull(channel.readInbound());

        ByteBuf half_identity = Unpooled.buffer();
        half_identity.writeByte(7);
        half_identity.writeByte(0);
        half_identity.writeBytes("foo".getBytes());

        channel.writeInbound(half_identity);
        Assert.assertNull(channel.readInbound());
    }

    @Test
    public void testMalformedClientIdentity() throws Exception {
        byte[] serverIdentity = "third_thing".getBytes();

        ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdentity);
        ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

        // Someone connects
        channel = new EmbeddedChannel(zfd);

        Object msg = channel.readOutbound();
        Assert.assertTrue(msg instanceof ByteBuf);
        Assert.assertArrayEquals(
                ((ByteBuf) msg).array(),
                makeFrame(serverIdentity).array());

        ByteBuf malformed = Unpooled.buffer();
        malformed.writeByte(7);
        malformed.writeByte(0x01);
        malformed.writeBytes("foobar".getBytes());

        channel.writeInbound(malformed);
        Assert.assertNull(channel.readInbound());
    }

    /**
     * Lets test the funky special case where a shorter length than 255 is encoded in a
     * big endian long, which MAY be done according to spec.
     */
    @Test
    public void testOverlyLongLength() throws Exception {
        byte[] serverIdentity = "fourth".getBytes();
        byte[] clientIdentity = "fifth".getBytes();

        ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdentity);
        ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

        // Someone connects
        channel = new EmbeddedChannel(zfd);

        Object msg = channel.readOutbound();
        Assert.assertTrue(msg instanceof ByteBuf);
        Assert.assertArrayEquals(
                ((ByteBuf) msg).array(),
                makeFrame(serverIdentity).array());

        ByteBuf funky = Unpooled.buffer();
        funky.writeBytes(new byte[]{(byte) 0xff, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                (byte) 0, (byte) 0, (byte) 0, (byte) (clientIdentity.length + 1), (byte) 0});
        funky.writeBytes(clientIdentity);

        channel.writeInbound(funky);
        Assert.assertNull(channel.readInbound());
    }


    /**
     * The ZMTP/1.0 spec states that "An identity greeting consists of a unique string of 1
     * to 255 octets". Let's make sure we don't accept longer identities.
     *
     * @throws Exception
     */
    @Test(expected = DecoderException.class)
    public void testOverlyLongIdentity() throws Exception {
        byte[] overlyLong = new byte[256];
        Arrays.fill(overlyLong, (byte) 'a');
        doHandshake("server".getBytes(), overlyLong);
    }

    /**
     * Verify that identity frames with the more flag set is allowed. This is needed for ZMTP 1.0
     * detection according to 15/ZMTP. http://rfc.zeromq.org/spec:15
     */
    @Test
    public void testIdentityWithMoreFlag() throws Exception {
        doHandshake("something".getBytes(), "another_thing".getBytes(), true);
    }

    private ZMTPFramingDecoder doHandshake(byte[] serverIdent, byte[] clientIdent) throws Exception {
        return doHandshake(serverIdent, clientIdent, false);
    }

    private ZMTPFramingDecoder doHandshake(byte[] serverIdent, byte[] clientIdent, boolean more)
            throws Exception {
        ZMTPSession s = new ZMTPSession(ZMTPConnectionType.Addressed, serverIdent);
        ZMTPFramingDecoder zfd = new ZMTPFramingDecoder(s);

        // Someone connects
        channel = new EmbeddedChannel(zfd);

        Object msg = channel.readOutbound();
        Assert.assertTrue(msg instanceof ByteBuf);
        Assert.assertArrayEquals(
                ((ByteBuf) msg).array(),
                makeFrame(serverIdent).array());

        Assert.assertNull(channel.readOutbound());

        // send it
        channel.writeInbound(makeFrame(clientIdent, more));

        Object out = channel.readOutbound();
        Assert.assertNull(out);

        return zfd;
    }

    private ByteBuf makeFrame(byte[] identity) {
        return makeFrame(identity, false);
    }

    private ByteBuf makeFrame(byte[] identity, boolean more) {
        if (identity == null) {
            identity = new byte[0];
        }
        ByteBuf cb = Unpooled.buffer(identity.length + 2);
        long l = identity.length + 1;
        if (l < 253) {
            cb.writeByte((byte) l);
        } else {
            cb.writeByte(0xff);
            if (cb.order() == ByteOrder.BIG_ENDIAN) {
                cb.writeLong(l);
            } else {
                cb.writeLong(ByteBufUtil.swapLong(l));
            }
        }

        cb.writeByte(more ? 0x01 : 0x00);
        cb.writeBytes(identity);
        return cb;
    }

}
