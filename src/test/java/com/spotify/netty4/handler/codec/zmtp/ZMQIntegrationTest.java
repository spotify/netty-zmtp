/*
* Copyright (c) 2012-2013 Spotify AB
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

import io.netty.util.ReferenceCountUtil;

import static com.spotify.netty4.handler.codec.zmtp.ZMTPProtocols.ZMTP20;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.DEALER;
import static com.spotify.netty4.handler.codec.zmtp.ZMTPSocketType.ROUTER;
import static io.netty.util.CharsetUtil.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ZMQIntegrationTest {

  private static final String ZMQ_IDENTITY = "zmq";
  private static final String ZMQ_ANONYMOUS = "";
  private static final String NETTY_IDENTITY = "netty";
  private static final String NETTY_ANONYMOUS = "";

  @Parameterized.Parameters(name = "identites: zmq=\"{0}\" netty=\"{1}\"")
  public static Object[][] identities() {
    return new Object[][]{
        {ZMQ_IDENTITY, NETTY_IDENTITY},
        {ZMQ_IDENTITY, NETTY_ANONYMOUS},
        {ZMQ_ANONYMOUS, NETTY_IDENTITY},
        {ZMQ_ANONYMOUS, NETTY_ANONYMOUS}
    };
  }

  @Parameterized.Parameter(0)
  public String zmqIdentity;

  @Parameterized.Parameter(1)
  public String nettyIdentity;

  private ZMQ.Context context;

  private ZMTPServer server;
  private ZMTPClient client;

  private int port;
  private ZMQ.Socket socket;

  @Before
  public void setUp() {
    context = ZMQ.context(1);
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.close();
    }
    if (client != null) {
      client.close();
    }
    if (socket != null) {
      socket.close();
    }
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void test_NettyBindRouter_ZmqConnectDealer()
      throws TimeoutException, InterruptedException {
    final ZMTPSocket router = nettyBind(ROUTER);
    final ZMQ.Socket dealer = zmqConnect(ZMQ.DEALER);
    testReqRep(dealer, router);
  }

  @Test
  public void test_NettyBindDealer_ZmqConnectRouter()
      throws InterruptedException, TimeoutException {
    final ZMTPSocket dealer = nettyBind(DEALER);
    final ZMQ.Socket router = zmqConnect(ZMQ.ROUTER);
    testReqRep(dealer, router);
  }

  @Test
  public void test_ZmqBindRouter_NettyConnectDealer()
      throws InterruptedException, TimeoutException {
    final ZMQ.Socket router = zmqBind(ZMQ.ROUTER);
    final ZMTPClient dealer = nettyConnect(DEALER);
    testReqRep(dealer, router);
  }

  @Test
  public void test_ZmqBindDealer_NettyConnectRouter()
      throws TimeoutException, InterruptedException {
    final ZMQ.Socket dealer = zmqBind(ZMQ.DEALER);
    final ZMTPClient router = nettyConnect(ROUTER);
    testReqRep(dealer, router);
  }

  private void testReqRep(final ZMQ.Socket req, final ZMTPSocket rep)
      throws InterruptedException, TimeoutException {
    verifyRemoteIdentity(rep);

    // Send request
    final ZMsg request = ZMsg.newStringMsg("envelope", "", "hello", "world");
    request.send(req, false);

    // Receive request
    final ZMTPIncomingMessage receivedRequest = rep.recv();

    // Send reply
    rep.send(receivedRequest.message());

    // Receive reply
    final ZMsg reply = ZMsg.recvMsg(req);

    // Verify echo
    assertEquals(request, reply);
  }

  private void testReqRep(final ZMTPSocket req, final ZMQ.Socket rep)
      throws InterruptedException, TimeoutException {
    verifyRemoteIdentity(req);

    // Send request
    final ZMTPMessage request = ZMTPMessage.fromUTF8("envelope", "", "hello", "world");
    ReferenceCountUtil.releaseLater(request.retain());
    req.send(request);

    // Receive request
    final ZMsg receivedRequest = ZMsg.recvMsg(rep);

    // Send reply
    receivedRequest.send(rep, false);

    // Receive reply
    final ZMTPIncomingMessage reply = req.recv();
    ReferenceCountUtil.releaseLater(reply);

    // Verify echo
    assertEquals(request, reply.message());
  }

  private ZMQ.Socket zmqBind(final int zmqType) {
    socket = context.socket(zmqType);
    setIdentity(socket);
    port = socket.bindToRandomPort("tcp://127.0.0.1");
    return socket;
  }

  private ZMQ.Socket zmqConnect(final int zmqType) {
    socket = context.socket(zmqType);
    setIdentity(socket);
    socket.connect(server.endpoint());
    return socket;
  }

  private ZMTPClient nettyConnect(final ZMTPSocketType socketType) {
    final ZMTPCodec codec = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .socketType(socketType)
        .localIdentity(nettyIdentity)
        .build();

    client = new ZMTPClient(codec, new InetSocketAddress("127.0.0.1", port));
    client.start();
    return client;
  }

  private ZMTPSocket nettyBind(final ZMTPSocketType socketType) {
    final ZMTPCodec serverCodec = ZMTPCodec.builder()
        .protocol(ZMTP20)
        .socketType(socketType)
        .localIdentity(nettyIdentity)
        .build();

    server = new ZMTPServer(serverCodec);
    server.start();

    return server;
  }

  private void setIdentity(final ZMQ.Socket socket) {
    if (!zmqIdentity.equals(ZMQ_ANONYMOUS)) {
      socket.setIdentity(zmqIdentity.getBytes(UTF_8));
    }
  }

  private void verifyRemoteIdentity(final ZMTPSocket socket) throws InterruptedException {
    if (zmqIdentity.equals(ZMQ_ANONYMOUS)) {
      return;
    }
    assertThat(socket.remoteIdentity(), is(UTF_8.encode(zmqIdentity)));
  }
}
