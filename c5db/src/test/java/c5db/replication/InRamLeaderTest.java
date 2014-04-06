/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.replication;

import c5db.interfaces.ReplicationModule;
import c5db.log.InRamLog;
import c5db.log.ReplicatorLog;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.AppendEntriesReply;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.ThrowFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.CharsetUtil;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static c5db.AsyncChannelAsserts.ChannelListener;
import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.IndexCommitMatchers.hasCommitNoticeIndexValueAtLeast;
import static c5db.replication.ReplicatorInstance.State;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class InRamLeaderTest {
  private static final long PEER_ID = 1;
  private static final long CURRENT_TERM = 4;
  private static final String QUORUM_ID = "quorumId";
  private static final List<ByteBuffer> TEST_DATUM = Lists.newArrayList(
      ByteBuffer.wrap("test".getBytes(CharsetUtil.UTF_8)));
  private static final long TEST_TIMEOUT = 5;

  private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel = new MemoryRequestChannel<>();
  private final Map<Long, BlockingQueue<Request<RpcRequest, RpcWireReply>>> requests = new HashMap<>();

  private final Map<Long, Callback<Request<RpcRequest, RpcWireReply>>> peerBehaviorialCallbacks = new HashMap<>();

  private final Channel<ReplicationModule.IndexCommitNotice> commitNotices = new MemoryChannel<>();

  @Rule
  public ThrowFiberExceptions fiberExceptionHandler = new ThrowFiberExceptions();

  private RunnableExecutor runnableExecutor = new RunnableExecutorImpl(
      new ExceptionHandlingBatchExecutor(fiberExceptionHandler));

  private final ReplicatorLog log = new InRamLog();
  private Fiber rpcFiber = new ThreadFiber(runnableExecutor, null, true);
  private ChannelListener<ReplicationModule.IndexCommitNotice> commitListener;

  private final List<Long> peerIdList = ImmutableList.of(1L, 2L, 3L);

  private ReplicatorInstance replicatorInstance;

  @Before
  public final void before() {
    commitListener = listenTo(commitNotices);

    sendRpcChannel.subscribe(rpcFiber, (request) -> System.out.println(request.getRequest()));
    sendRpcChannel.subscribe(rpcFiber, this::routeOutboundRequests);

    InRamSim.Info info = new InRamSim.Info(0);
    info.startTimeout();

    replicatorInstance = new ReplicatorInstance(new ThreadFiber(runnableExecutor, null, true),
        PEER_ID,
        QUORUM_ID,
        peerIdList,
        log,
        info,
        new InRamSim.Persister(),
        sendRpcChannel,
        new MemoryChannel<>(),
        commitNotices,
        CURRENT_TERM,
        State.LEADER,
        0,
        PEER_ID,
        PEER_ID);
    replicatorInstance.start();
    rpcFiber.start();
  }

  @After
  public final void after() {
    commitListener.dispose();
    replicatorInstance.dispose();
    rpcFiber.dispose();
  }

  @Test
  public void shouldCommitIfOnePeerAcksAllAppendEntryMessages() throws Throwable {
    ackAllRequestsToPeer(2);
    replicatorInstance.logData(TEST_DATUM);
    replicatorInstance.logData(TEST_DATUM);
    assertEventually(commitListener, hasCommitNoticeIndexValueAtLeast((long) 2));
  }

  @Test
  public void testFollowerCatchup() throws Throwable {
    long flakyPeer = 2;
    ackAllRequestsToPeer(3);

    // Peer 3 replies true to all AppendEntries requests; this causes the leader to commit, since
    // together with peer 3 it has a majority. Flaky peer 2 replies true to acknowledge receiving the
    // first entry.
    createRequestRule(flakyPeer, (request) -> {
      long commitIndex = request.getRequest().getAppendMessage().getCommitIndex();
      if (commitIndex > 1) {
        peerBehaviorialCallbacks.remove(flakyPeer);
        return;
      }
      sendAppendEntriesReply_Success(request);
    });
    replicatorInstance.logData(TEST_DATUM);
    assertEventually(commitListener, hasCommitNoticeIndexValueAtLeast(1));

    // The next two entries don't reach the flaky peer, but they are acked and committed by the good peer.
    createRequestRule(flakyPeer, (request) -> {
    });
    replicatorInstance.logData(TEST_DATUM);
    replicatorInstance.logData(TEST_DATUM);
    assertEventually(commitListener, hasCommitNoticeIndexValueAtLeast(3));

    // The leader must now detect the last entry received by the flaky peer, which is 1; when it does that,
    // the test succeeds.
    SettableFuture<Boolean> waitCond = SettableFuture.create();
    createRequestRule(flakyPeer, (request) -> {
      long prevLogIndex = request.getRequest().getAppendMessage().getPrevLogIndex();
      if (prevLogIndex == 1) {
        waitCond.set(true);
      } else {
        sendAppendEntriesReply_Failure(request);
      }
    });

    // Test fails iff this throws TimeoutException
    waitCond.get(TEST_TIMEOUT, TimeUnit.SECONDS);
  }

  @Test
  public void testFollowerWhoReceivedNothing() throws Throwable {
    long flakyPeer = 2;
    ackAllRequestsToPeer(3);

    // flaky peer drops all requests:
    createRequestRule(flakyPeer, (request) -> {});
    for (int i = 1; i <= 10; i++) {
      replicatorInstance.logData(TEST_DATUM);
    }
    assertEventually(commitListener, hasCommitNoticeIndexValueAtLeast(10));

    // At this point, the leader has sent ten requests and has committed them, but has not heard anything
    // from one of its followers. Now that follower returns false, and the leader starts stepping back
    // through log entries to find the most recent one it has in common with that follower. (Answer: none).
    SettableFuture<Boolean> waitCond = SettableFuture.create();

    createRequestRule(flakyPeer, (request) -> {
      try {
        AppendEntries msg = request.getRequest().getAppendMessage();

        // Leader should only be sending AppendEntries
        //assertNotNull(msg);
        assertThat(msg, is(not(nullValue())));

        // Return false until prevLogIndex = 0
        if (msg.getPrevLogIndex() > 0) {
          sendAppendEntriesReply_Failure(request);
        } else {
          // End test; check that the leader is correctly sending the first entry (at very least)
          int numberEntriesSent = msg.getEntriesList().size();

          assertEquals(0, msg.getPrevLogIndex());
          assertNotEquals(0, numberEntriesSent);
          assertEquals(1, msg.getEntriesList().get(0).getIndex());
          assertEquals(numberEntriesSent, msg.getEntriesList().get(numberEntriesSent - 1).getIndex());
          peerBehaviorialCallbacks.remove(flakyPeer);
          waitCond.set(true);
        }
      } catch (Throwable t) {
        // Catch assertion errors and forward to the test thread.
        waitCond.setException(t);
      }
    });

    assertTrue(waitCond.get(TEST_TIMEOUT, TimeUnit.SECONDS));
  }


  /**
   * Either route an outbound request to a callback, or queue it, depending on destination peer
   */
  private void routeOutboundRequests(Request<RpcRequest, RpcWireReply> request) {
    long to = request.getRequest().to;
    if (peerBehaviorialCallbacks.containsKey(to)) {
      peerBehaviorialCallbacks.get(to).onMessage(request);
    } else {
      if (!requests.containsKey(to)) {
        requests.put(to, new LinkedBlockingQueue<>());
      }
      requests.get(to).add(request);
    }
  }

  /**
   * Reply true or false to a single AppendEntries request
   */
  private void sendAppendEntriesReply(Request<RpcRequest, RpcWireReply> request, boolean trueIsSuccessFlag) {
    RpcRequest message = request.getRequest();
    long termFromMessage = message.getAppendMessage().getTerm();
    RpcWireReply reply = new RpcWireReply(message.to, QUORUM_ID,
        new AppendEntriesReply(termFromMessage, trueIsSuccessFlag, 0));
    request.reply(reply);
  }

  private void sendAppendEntriesReply_Success(Request<RpcRequest, RpcWireReply> request) {
    sendAppendEntriesReply(request, true);
  }

  private void sendAppendEntriesReply_Failure(Request<RpcRequest, RpcWireReply> request) {
    sendAppendEntriesReply(request, false);
  }



  /**
   * Execute a callback on all pending requests to a given peer, and any requests hereafter
   */
  private void createRequestRule(long peerId, Callback<Request<RpcRequest, RpcWireReply>> handler) throws Exception {
    peerBehaviorialCallbacks.put(peerId, handler);
    if (requests.containsKey(peerId)) {
      List<Request<RpcRequest, RpcWireReply>> pendingRequests = new LinkedList<>();
      requests.get(peerId).drainTo(pendingRequests);
      for (Request<RpcRequest, RpcWireReply> request : pendingRequests) {
        handler.onMessage(request);
      }
    }
  }

  /**
   * Reply true to all pending requests to the given follower, and any requests hereafter
   */
  private void ackAllRequestsToPeer(long peerId) throws Exception {
    createRequestRule(peerId, this::sendAppendEntriesReply_Success);
  }
}
