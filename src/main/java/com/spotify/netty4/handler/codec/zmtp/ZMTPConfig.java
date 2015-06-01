/*
 * Copyright (c) 2012-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.netty4.handler.codec.zmtp;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPUtils.checkNotNull;
import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Configuration for a ZMTP session and {@link ZMTPCodec}. Can be reused and shared across multiple
 * channel instances.
 */
public class ZMTPConfig {

  public static final ByteBuffer ANONYMOUS = ByteBuffer.allocate(0).asReadOnlyBuffer();

  private final ZMTPProtocol protocol;
  private final boolean interop;
  private final ZMTPSocketType socketType;
  private final ByteBuffer localIdentity;
  private final ZMTPEncoder.Factory encoder;
  private final ZMTPDecoder.Factory decoder;
  private final ZMTPIdentityGenerator identityGenerator;

  private ZMTPConfig(final Builder builder) {
    this.protocol = checkNotNull(builder.protocol, "protocol");
    this.interop = checkNotNull(builder.interop, "interop");
    this.socketType = checkNotNull(builder.socketType, "socketType");
    this.localIdentity = checkNotNull(builder.localIdentity, "localIdentity");
    this.encoder = checkNotNull(builder.encoder, "encoder");
    this.decoder = checkNotNull(builder.decoder, "decoder");
    this.identityGenerator = checkNotNull(builder.identityGenerator, "identityGenerator");
  }

  public ZMTPProtocol protocol() {
    return protocol;
  }

  public boolean interop() {
    return interop;
  }

  public ZMTPSocketType socketType() {
    return socketType;
  }

  public ByteBuffer localIdentity() {
    return localIdentity;
  }

  public ZMTPEncoder.Factory encoder() {
    return encoder;
  }

  public ZMTPDecoder.Factory decoder() {
    return decoder;
  }

  public ZMTPIdentityGenerator identityGenerator() {
    return identityGenerator;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private ZMTPProtocol protocol = ZMTPProtocols.ZMTP20;
    private boolean interop = true;
    private ZMTPSocketType socketType;
    private ByteBuffer localIdentity = ANONYMOUS;
    private ZMTPEncoder.Factory encoder = ZMTPMessageEncoder.FACTORY;
    private ZMTPDecoder.Factory decoder = ZMTPMessageDecoder.FACTORY;
    private ZMTPIdentityGenerator identityGenerator = ZMTPLongIdentityGenerator.GLOBAL;

    private Builder() {
    }

    private Builder(final ZMTPConfig config) {
      this.protocol = config.protocol;
      this.interop = config.interop;
      this.socketType = config.socketType;
      this.localIdentity = config.localIdentity;
      this.encoder = config.encoder;
      this.decoder = config.decoder;

    }

    public Builder protocol(final ZMTPProtocol protocol) {
      this.protocol = protocol;
      return this;
    }

    public Builder interop(final boolean interop) {
      this.interop = interop;
      return this;
    }

    public Builder socketType(final ZMTPSocketType socketType) {
      this.socketType = socketType;
      return this;
    }

    public Builder localIdentity(final CharSequence localIdentity) {
      return localIdentity(UTF_8.encode(localIdentity.toString()));
    }

    public Builder localIdentity(final byte[] localIdentity) {
      return localIdentity(ByteBuffer.wrap(localIdentity));
    }

    public Builder localIdentity(final ByteBuffer localIdentity) {
      this.localIdentity = localIdentity;
      return this;
    }

    public Builder encoder(final ZMTPEncoder.Factory encoder) {
      this.encoder = encoder;
      return this;
    }

    public Builder encoder(final Class<? extends ZMTPEncoder> encoder) {
      return encoder(new ZMTPEncoderClassFactory(encoder));
    }

    public Builder decoder(final ZMTPDecoder.Factory decoder) {
      this.decoder = decoder;
      return this;
    }

    public Builder decoder(final Class<? extends ZMTPDecoder> decoder) {
      return decoder(new ZMTPDecoderClassFactory(decoder));
    }

    public Builder identityGenerator(final ZMTPIdentityGenerator identityGenerator) {
      this.identityGenerator = identityGenerator;
      return this;
    }

    public ZMTPConfig build() {
      return new ZMTPConfig(this);
    }

  }

  @Override
  public String toString() {
    return "ZMTPConfig{" +
           "protocol=" + protocol +
           ", interop=" + interop +
           ", socketType=" + socketType +
           ", localIdentity=" + localIdentity +
           ", encoder=" + encoder +
           ", decoder=" + decoder +
           '}';
  }

  private static class ZMTPEncoderClassFactory implements ZMTPEncoder.Factory {

    private final Constructor<? extends ZMTPEncoder> constructor;

    public ZMTPEncoderClassFactory(final Class<? extends ZMTPEncoder> encoder) {
      checkNotNull(encoder, "encoder");
      try {
        constructor = encoder.getDeclaredConstructor();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Class must have default constructor: " + encoder);
      }
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
    }

    @Override
    public ZMTPEncoder encoder(final ZMTPSession session) {
      try {
        return constructor.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String toString() {
      return "ZMTPEncoderClassFactory{" +
             "constructor=" + constructor +
             '}';
    }
  }

  private static class ZMTPDecoderClassFactory implements ZMTPDecoder.Factory {

    private final Constructor<? extends ZMTPDecoder> constructor;

    public ZMTPDecoderClassFactory(final Class<? extends ZMTPDecoder> decoder) {
      checkNotNull(decoder, "decoder");
      try {
        constructor = decoder.getDeclaredConstructor();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Class must have default constructor: " + decoder);
      }
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
    }

    @Override
    public ZMTPDecoder decoder(final ZMTPSession session) {
      try {
        return constructor.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String toString() {
      return "ZMTPDecoderClassFactory{" +
             "constructor=" + constructor +
             '}';
    }
  }
}
