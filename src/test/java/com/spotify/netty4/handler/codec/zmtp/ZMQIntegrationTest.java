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

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP10;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@RunWith(Theories.class)
public class ZMQIntegrationTest {

  private static final String ANONYMOUS = "";
  private static final String IDENTITY = "zmq-integration-test";
  private static final String MIN_IDENTITY = "z";
  private static final String MAX_IDENTITY = Strings.repeat("z", 255);

  @DataPoints("identities")
  public static final String[] IDENTITIES = {IDENTITY, MIN_IDENTITY, MAX_IDENTITY, ANONYMOUS};

  @DataPoints("versions")
  public static final ZMTPProtocol[] PROTOCOLS = {ZMTP10, ZMTP20};

  private final ZMTPSocket.Handler handler =
      Mockito.mock(ZMTPSocket.Handler.class);
  private final ArgumentCaptor<ZMTPMessage> messageCaptor =
      ArgumentCaptor.forClass(ZMTPMessage.class);

  private ZMQ.Context context;

  private ZMTPSocket zmtpSocket;

  private int port;
  private ZMQ.Socket zmqSocket;

  @Before
  public void setUp() {
    context = ZMQ.context(1);
  }

  @After
  public void tearDown() {
    if (zmtpSocket != null) {
      zmtpSocket.close();
    }
    if (zmqSocket != null) {
      zmqSocket.close();
    }
    if (context != null) {
      context.close();
    }
  }

  @Theory
  public void test_NettyBindRouter_ZmqConnectDealer(
      @FromDataPoints("identities") final String zmqIdentity,
      @FromDataPoints("identities") final String nettyIdentity,
      @FromDataPoints("versions") final ZMTPProtocol nettyProtocol
  )
      throws TimeoutException, InterruptedException, ExecutionException {
    // XXX (dano): jeromq fails on identities longer than 127 bytes due to a signedness issue
    assumeFalse(MAX_IDENTITY.equals(zmqIdentity));

    final ZMTPSocket router = nettyBind(ROUTER, nettyIdentity, nettyProtocol);
    final ZMQ.Socket dealer = zmqConnect(ZMQ.DEALER, zmqIdentity);

    testReqRep(dealer, router, zmqIdentity);
  }

  @Theory
  public void test_NettyBindDealer_ZmqConnectRouter(
      @FromDataPoints("identities") final String zmqIdentity,
      @FromDataPoints("identities") final String nettyIdentity,
      @FromDataPoints("versions") final ZMTPProtocol nettyProtocol
  )
      throws InterruptedException, TimeoutException, ExecutionException {
    // XXX (dano): jeromq fails on identities longer than 127 bytes due to a signedness issue
    assumeFalse(MAX_IDENTITY.equals(zmqIdentity));

    final ZMTPSocket dealer = nettyBind(DEALER, nettyIdentity, nettyProtocol);
    final ZMQ.Socket router = zmqConnect(ZMQ.ROUTER, zmqIdentity);
    testReqRep(dealer, router, zmqIdentity);
  }

  @Theory
  public void test_ZmqBindRouter_NettyConnectDealer(
      @FromDataPoints("identities") final String zmqIdentity,
      @FromDataPoints("identities") final String nettyIdentity,
      @FromDataPoints("versions") final ZMTPProtocol nettyProtocol
  )
      throws InterruptedException, TimeoutException, ExecutionException {
    // XXX (dano): jeromq fails on identities longer than 127 bytes due to a signedness issue
    assumeFalse(MAX_IDENTITY.equals(zmqIdentity));

    final ZMQ.Socket router = zmqBind(ZMQ.ROUTER, zmqIdentity);
    final ZMTPSocket dealer = nettyConnect(DEALER, nettyIdentity, nettyProtocol);
    testReqRep(dealer, router, zmqIdentity);
  }

  @Theory
  public void test_ZmqBindDealer_NettyConnectRouter(
      @FromDataPoints("identities") final String zmqIdentity,
      @FromDataPoints("identities") final String nettyIdentity,
      @FromDataPoints("versions") final ZMTPProtocol nettyProtocol
  )
      throws TimeoutException, InterruptedException, ExecutionException {
    // XXX (dano): jeromq fails on identities longer than 127 bytes due to a signedness issue
    assumeFalse(MAX_IDENTITY.equals(zmqIdentity));

    final ZMQ.Socket dealer = zmqBind(ZMQ.DEALER, zmqIdentity);
    final ZMTPSocket router = nettyConnect(ROUTER, nettyIdentity, nettyProtocol);
    testReqRep(dealer, router, zmqIdentity);
  }

  private void testReqRep(final ZMQ.Socket req, final ZMTPSocket rep, final String zmqIdentity)
      throws InterruptedException, TimeoutException {

    // Verify that sockets are connected
    verify(handler, timeout(5000)).connected(eq(rep), any(ZMTPSocket.ZMTPPeer.class));

    // Verify that the peer identity was correctly received
    verifyPeerIdentity(rep, zmqIdentity);

    // Send request
    final ZMsg request = ZMsg.newStringMsg("envelope", "", "hello", "world");
    request.send(req, false);

    // Receive request
    verify(handler, timeout(5000)).message(
        eq(rep), any(ZMTPSocket.ZMTPPeer.class), messageCaptor.capture());
    final ZMTPMessage receivedRequest = messageCaptor.getValue();

    // Send reply
    rep.send(receivedRequest);

    // Receive reply
    final ZMsg reply = ZMsg.recvMsg(req);

    // Verify echo
    assertEquals(request, reply);
  }

  private void testReqRep(final ZMTPSocket req, final ZMQ.Socket rep, final String zmqIdentity)
      throws InterruptedException, TimeoutException {

    // Verify that sockets are connected
    verify(handler, timeout(5000)).connected(eq(req), any(ZMTPSocket.ZMTPPeer.class));

    // Verify that the peer identity was correctly received
    verifyPeerIdentity(req, zmqIdentity);

    // Send request
    final ZMTPMessage request = ZMTPMessage.fromUTF8("envelope", "", "hello", "world");
    request.retain();
    req.send(request);

    // Receive request
    final ZMsg receivedRequest = ZMsg.recvMsg(rep);

    // Send reply
    receivedRequest.send(rep, false);

    // Receive reply
    verify(handler, timeout(5000)).message(
        eq(req), any(ZMTPSocket.ZMTPPeer.class), messageCaptor.capture());
    final ZMTPMessage reply = messageCaptor.getValue();
    ReferenceCountUtil.releaseLater(reply);

    // Verify echo
    assertEquals(request, reply);
    request.release();
  }

  private ZMQ.Socket zmqBind(final int zmqType, final String identity) {
    zmqSocket = context.socket(zmqType);
    setIdentity(zmqSocket, identity);
    port = zmqSocket.bindToRandomPort("tcp://127.0.0.1");
    return zmqSocket;
  }

  private ZMQ.Socket zmqConnect(final int zmqType, final String identity) {
    zmqSocket = context.socket(zmqType);
    setIdentity(zmqSocket, identity);
    zmqSocket.connect("tcp://127.0.0.1:" + port);
    return zmqSocket;
  }

  private ZMTPSocket nettyConnect(final ZMTPSocketType socketType, final String identity,
                                  final ZMTPProtocol protocol)
      throws InterruptedException, ExecutionException, TimeoutException {
    zmtpSocket = ZMTPSocket.builder()
        .handler(handler)
        .protocol(protocol)
        .type(socketType)
        .identity(identity)
        .build();

    final ListenableFuture<Void> f = zmtpSocket.connect("tcp://127.0.0.1:" + port);
    f.get(5, TimeUnit.SECONDS);

    return zmtpSocket;
  }

  private ZMTPSocket nettyBind(final ZMTPSocketType socketType, final String identity,
                               final ZMTPProtocol protocol)
      throws InterruptedException, ExecutionException, TimeoutException {
    zmtpSocket = ZMTPSocket.builder()
        .handler(handler)
        .protocol(protocol)
        .type(socketType)
        .identity(identity)
        .build();

    final ListenableFuture<InetSocketAddress> f = zmtpSocket.bind("tcp://127.0.0.1:*");
    final InetSocketAddress address = f.get(5, TimeUnit.SECONDS);
    port = address.getPort();

    return zmtpSocket;
  }

  private void setIdentity(final ZMQ.Socket socket, final String identity) {
    if (!identity.equals(ANONYMOUS)) {
      socket.setIdentity(identity.getBytes(UTF_8));
    }
  }

  private void verifyPeerIdentity(final ZMTPSocket socket, final String zmqIdentity) throws InterruptedException {
    final List<ZMTPSocket.ZMTPPeer> peers = socket.peers();
    assertThat(peers.size(), is(1));
    final ZMTPSession session = peers.get(0).session();
    if (ANONYMOUS.equals(zmqIdentity)) {
      assertThat(session.isPeerAnonymous(), is(true));
      assertThat(UTF_8.decode(session.peerIdentity()).toString(), not(isEmptyString()));
    } else {
      assertThat(session.isPeerAnonymous(), is(false));
      assertThat(session.peerIdentity(), is(UTF_8.encode(zmqIdentity)));
    }
  }
}
