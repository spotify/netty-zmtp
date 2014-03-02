package com.spotify.netty.handler.codec.zmtp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * An abstract base class for common functionality to the ZMTP codecs.
 */
abstract class CodecBase extends ReplayingDecoder<Void> {

    protected final ZMTPSession session;
    protected HandshakeListener listener;

    CodecBase(ZMTPSession session) {
        this.session = session;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx)
            throws Exception {

        setListener(new HandshakeListener() {
            @Override
            public void handshakeDone(int protocolVersion, byte[] remoteIdentity) {
                session.setRemoteIdentity(remoteIdentity);
                session.setActualVersion(protocolVersion);
                updatePipeline(ctx.pipeline(), session);
                ctx.fireChannelActive();
            }
        });

        Channel channel = ctx.channel();

        channel.writeAndFlush(onConnect());
        session.setChannel(channel);
    }

    abstract ByteBuf onConnect();

    abstract ByteBuf inputOutput(final ByteBuf buffer) throws ZMTPException;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        buffer.markReaderIndex();
        ByteBuf toSend = inputOutput(buffer);
        while (toSend != null) {
            ctx.channel().writeAndFlush(toSend);
            toSend = inputOutput(buffer);
        }
        // This follows the pattern for dynamic pipelines documented in
        // http://netty.io/3.6/api/org/jboss/netty/handler/codec/replay/ReplayingDecoder.html
        if (actualReadableBytes() > 0) {
            out.add(buffer.readBytes(actualReadableBytes()));
        }
    }

    void setListener(HandshakeListener listener) {
        this.listener = listener;
    }


    private void updatePipeline(ChannelPipeline pipeline,
                                ZMTPSession session) {
        pipeline.addAfter(pipeline.context(this).name(), "zmtpEncoder",
                new ZMTPFramingEncoder(session));
        pipeline.addAfter("zmtpEncoder", "zmtpDecoder",
                new ZMTPFramingDecoder(session));
        pipeline.remove(this);
    }

    /**
     * Parse and return the remote identity octets from a ZMTP/1.0 greeting.
     */
    static byte[] readZMTP1RemoteIdentity(final ByteBuf buffer) throws ZMTPException {
        buffer.markReaderIndex();

        final long len = ZMTPUtils.decodeLength(buffer);
        if (len > 256) {
            // spec says the ident string can be up to 255 chars
            throw new ZMTPException("Remote identity longer than the allowed 255 octets");
        }

        // Bail out if there's not enough data
        if (len == -1 || buffer.readableBytes() < len) {
            buffer.resetReaderIndex();
            throw new IndexOutOfBoundsException("not enough data");
        }
        // skip the flags byte
        buffer.skipBytes(1);

        if (len == 1) {
            return null;
        }
        final byte[] identity = new byte[(int) len - 1];
        buffer.readBytes(identity);
        return identity;
    }
}
