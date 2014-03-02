package com.spotify.netty.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests related to CodecBase and ZMTP*Codec
 */
public class CodecTest {
    @Test
    public void testOverlyLongIdentity() throws Exception {
        byte[] overlyLong = new byte[256];
        Arrays.fill(overlyLong, (byte) 'a');
        ByteBuf buffer = Unpooled.buffer();
        ZMTPUtils.encodeLength(overlyLong.length + 1, buffer);
        buffer.writeByte(0);
        buffer.writeBytes(overlyLong);
        try {
            Assert.assertArrayEquals(overlyLong, CodecBase.readZMTP1RemoteIdentity(buffer));
            Assert.fail("Should have thrown exception");
        } catch (ZMTPException e) {
            //pass
        }
    }

}
