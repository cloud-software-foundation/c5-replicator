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

import c5db.interfaces.replication.IndexCommitNotice;
import c5db.log.InRamLog;
import c5db.log.ReplicatorLog;
import c5db.replication.generated.AppendEntriesReply;
import c5db.replication.generated.LogEntry;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.Callback;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static c5db.AsyncChannelAsserts.ChannelHistoryMonitor;
import static c5db.AsyncChannelAsserts.ChannelListener;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.IndexCommitMatchers.hasCommitNoticeIndexValueAtLeast;
import static c5db.RpcMatchers.RequestMatcher;
import static c5db.RpcMatchers.RequestMatcher.anAppendRequest;
import static c5db.interfaces.replication.Replicator.State;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.someData;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;


public class ReplicatorLeaderTest {
  private static final long LEADER_ID = 1;
  private static final long CURRENT_TERM = 4;
  private static final String QUORUM_ID = "quorumId";
  private static final List<Long> PEER_ID_LIST = ImmutableList.of(1L, 2L, 3L);
  private static final List<ByteBuffer> TEST_DATUM = Lists.newArrayList(someData());

  private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel = new MemoryRequestChannel<>();

  private final Map<Long, BlockingQueue<Request<RpcRequest, RpcWireReply>>> requests = new HashMap<>();
  private final Map<Long, Callback<Request<RpcRequest, RpcWireReply>>> peerBehavioralCallbacks = new HashMap<>();

  @Rule
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();
  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);
  private final Fiber rpcFiber = new ThreadFiber(new RunnableExecutorImpl(batchExecutor), "rpcFiber-Thread", true);

  private final Channel<IndexCommitNotice> commitNotices = new MemoryChannel<>();
  private final ChannelListener<IndexCommitNotice> commitListener = listenTo(commitNotices);
  private final MemoryChannel<Request<RpcRequest, RpcWireReply>> requestLog = new MemoryChannel<>();
  private final ChannelHistoryMonitor<Request<RpcRequest, RpcWireReply>> requestMonitor =
      new ChannelHistoryMonitor<>(requestLog, rpcFiber);
  private final ChannelHistoryMonitor<IndexCommitNotice> commitMonitor =
      new ChannelHistoryMonitor<>(commitNotices, rpcFiber);

  private final ReplicatorLog log = new InRamLog();

  private ReplicatorInstance replicatorInstance;
  private long lastIndex;

  @Before
  public final void createLeaderAndSetupFibersAndChannels() {
    sendRpcChannel.subscribe(rpcFiber, (request) -> System.out.println(request.getRequest()));
    sendRpcChannel.subscribe(rpcFiber, this::routeOutboundRequests);
    sendRpcChannel.subscribe(rpcFiber, requestLog::publish);

    Fiber replicatorFiber = new ThreadFiber(new RunnableExecutorImpl(batchExecutor), "replicatorFiber-Thread", true);
    InRamSim.Info info = new InRamSim.Info(0, 1000);
    info.startTimeout();

    log.logEntries(
        Lists.newArrayList(
            new LogEntry(CURRENT_TERM, 1, new ArrayList<>(), QuorumConfiguration.of(PEER_ID_LIST).toProtostuff())));
    lastIndex = 1;

    replicatorInstance = new ReplicatorInstance(replicatorFiber,
        LEADER_ID,
        QUORUM_ID,
        PEER_ID_LIST,
        log,
        info,
        new InRamSim.Persister(),
        sendRpcChannel,
        new MemoryChannel<>(),
        commitNotices,
        CURRENT_TERM,
        State.LEADER,
        seqNum(1),
        LEADER_ID,
        LEADER_ID);
    replicatorInstance.start();
    rpcFiber.start();
  }

  @After
  public final void disposeResources() {
    commitListener.dispose();
    replicatorInstance.dispose();
    rpcFiber.dispose();
  }

  @Test
  public void willCommitIfOneOutOfTwoOfItsPeersAcksAllAppendEntryMessages() throws Throwable {
    peer(2).willIgnoreAllRequests();
    peer(3).willReplyToAllRequestsWith(true);

    leader().logDataUpToIndex(5);

    expectLeaderToCommitUpToIndex(5);
  }

  @Test
  public void willFindTheLastEntryItHasInCommonWithAPeerAndSendTheNextEntryInSequence() throws Throwable {
    final long firstEntryIndexSent = lastIndexLogged() + 1;
    final long lastEntryIndexSent = 5;

    peer(3).willReplyToAllRequestsWith(true);

    leader().logSomeData();
    waitUntilLeaderSends(aRequestToPeer(2).withLastEntryLogged()).thenPeerWillReply(true);

    leader().logDataUpToIndex(lastEntryIndexSent);
    expectLeaderToCommitUpToIndex(lastEntryIndexSent);

    ignoringRequestsTheLeaderHasAlreadySent();
    peer(2).willReplyToAllRequestsFromNowOnWith(false);

    expectLeaderToSend(aRequestToPeer(2).withPrevLogIndex(firstEntryIndexSent));
  }

  @Test
  public void willFindIfAPeerDidNotReceiveAnyEntriesAndResendTheFirstEntryInSequence() throws Throwable {
    final long lastEntryIndexSent = 5;

    peer(2).willIgnoreAllRequests();
    peer(3).willReplyToAllRequestsWith(true);

    leader().logDataUpToIndex(lastEntryIndexSent);
    expectLeaderToCommitUpToIndex(lastEntryIndexSent);

    /* At this point, the leader has sent some requests and has committed them, but has not heard anything
     * from one of its followers. Now that follower returns false, and the leader starts stepping back
     * through log entries to find the most recent one it has in common with that follower. (Answer: none).
     */
    ignoringRequestsTheLeaderHasAlreadySent();
    peer(2).willReplyToAllRequestsFromNowOnWith(false);

    expectLeaderToSend(
        allOf(
            aRequestToPeer(2).withPrevLogIndex(0),
            aRequestToPeer(2).withLogIndex(1)));
    expectLeaderToSend(aRequestToPeer(2).withLogIndex(lastEntryIndexSent));
  }

  @Test
  public void commitsWhenAMajorityOfPeersHaveAckedEvenIfNoSingleIndexHasBeenAckedByAMajority() throws Throwable {
    /**
     * Tests the case where each of the leader and its two peers has logged up to a different index. The
     * leader should commit up to the middle index of the three, since a majority has logged it.
     */
    peer(3).willIgnoreAllRequests();

    leader().logSomeData();
    final SentRequest firstRequest = waitUntilLeaderSends(aRequestToPeer(2).withLastEntryLogged());

    leader().logSomeData();
    expectLeaderToSend(aRequestToPeer(2).withLastEntryLogged());

    replyTo(firstRequest, true);

    expectLeaderToCommitUpToIndex(firstRequest.lastEntryIndex());
  }

  @Test
  public void decrementsTheNextIndexOfAPeerWhenThePeerRepliesFalseWithANextIndexOfZero() throws Throwable {
    final long maxIndexLogged = 5;

    leader().logDataUpToIndex(maxIndexLogged);

    final SentRequest problematicRequest = waitUntilLeaderSends(aRequestToPeer(2).withLogIndex(maxIndexLogged));
    ignoringRequestsTheLeaderHasAlreadySent();

    replyTo(problematicRequest, false, 0);
    expectLeaderToSend(aRequestToPeer(2)
        .withLogIndex(maxIndexLogged - 1)
        .withPrevLogIndex(equalTo(maxIndexLogged - 2)));
  }

  @Test
  public void sendsTheRequestedNextEntryWhenThePeerRepliesFalseWithANonzeroNextIndex() throws Throwable {
    final long maxIndexLogged = 5;
    final long nextIndexPeerRepliesWith = 3;

    leader().logDataUpToIndex(maxIndexLogged);

    final SentRequest problematicRequest = waitUntilLeaderSends(aRequestToPeer(2).withLogIndex(maxIndexLogged));
    ignoringRequestsTheLeaderHasAlreadySent();

    replyTo(problematicRequest, false, nextIndexPeerRepliesWith);
    expectLeaderToSend(aRequestToPeer(2)
        .withLogIndex(nextIndexPeerRepliesWith)
        .withPrevLogIndex(equalTo(nextIndexPeerRepliesWith - 1)));
  }


  private long lastIndexLogged() {
    return lastIndex;
  }

  /**
   * Either route an outbound request to a callback, or queue it, depending on destination peer
   */
  private void routeOutboundRequests(Request<RpcRequest, RpcWireReply> request) {
    long to = request.getRequest().to;
    if (peerBehavioralCallbacks.containsKey(to)) {
      peerBehavioralCallbacks.get(to).onMessage(request);
    } else {
      if (!requests.containsKey(to)) {
        requests.put(to, new LinkedBlockingQueue<>());
      }
      requests.get(to).add(request);
    }
  }

  /**
   * Execute a callback on all pending requests to a given peer, and any requests hereafter
   */
  private void createRequestRule(long peerId, Callback<Request<RpcRequest, RpcWireReply>> handler) {
    peerBehavioralCallbacks.put(peerId, handler);
    if (requests.containsKey(peerId)) {
      List<Request<RpcRequest, RpcWireReply>> pendingRequests = new LinkedList<>();
      requests.get(peerId).drainTo(pendingRequests);
      pendingRequests.forEach(handler::onMessage);
    }
  }

  private RequestMatcherCurriedWithPeerId aRequestToPeer(long peerId) {
    return new RequestMatcherCurriedWithPeerId(peerId);
  }

  private class RequestMatcherCurriedWithPeerId {
    private final long peerId;

    public RequestMatcherCurriedWithPeerId(long peerId) {
      this.peerId = peerId;
    }

    public RequestMatcher withLastEntryLogged() {
      return withLogIndex(lastIndex);
    }

    public RequestMatcher withLogIndex(long index) {
      return anAppendRequest().to(peerId).containingEntryIndex(index);
    }

    public RequestMatcher withPrevLogIndex(long index) {
      return anAppendRequest().to(peerId).withPrevLogIndex(equalTo(index));
    }
  }

  private PeerSimulator peer(long peerId) {
    return new PeerSimulator(peerId);
  }

  private class PeerSimulator {
    private final long peerId;

    public PeerSimulator(long peerId) {
      this.peerId = peerId;
    }

    public void willReplyToAllRequestsWith(boolean success) {
      createRequestRule(peerId, (request) -> sendAppendEntriesReply(request, success, 0));
    }

    public void willReplyToAllRequestsFromNowOnWith(boolean success) {
      if (requests.containsKey(peerId)) {
        requests.get(peerId).clear();
      }
      createRequestRule(peerId, (request) -> sendAppendEntriesReply(request, success, 0));
    }

    public void willIgnoreAllRequests() {
      createRequestRule(peerId, (request) -> {
      });
    }
  }

  private LeaderController leader() {
    return new LeaderController();
  }


  private class LeaderController {
    private LeaderController logSomeData() throws Exception {
      lastIndex = replicatorInstance.logData(TEST_DATUM).get();

      // Wait for the leader to send this entry so that later entries don't get batched together into the
      // same append message -- this enables the AppendEntries reply to have more granularity about which
      // entries it acks.
      expectLeaderToSend(aRequestToPeer(2).withLastEntryLogged());
      return this;
    }

    private LeaderController logDataUpToIndex(long index) throws Exception {
      while (lastIndex < index) {
        logSomeData();
      }
      return this;
    }
  }

  private void expectLeaderToSend(Matcher<Request<RpcRequest, RpcWireReply>> requestMatcher) {
    requestMonitor.waitFor(requestMatcher);
  }

  private SentRequest waitUntilLeaderSends(Matcher<Request<RpcRequest, RpcWireReply>> requestMatcher) {
    return new SentRequest(
        requestMonitor.waitFor(requestMatcher));
  }

  private void ignoringRequestsTheLeaderHasAlreadySent() {
    requestMonitor.forgetHistory();
    requests.values().forEach(BlockingQueue::clear);
  }

  private class SentRequest {
    private final Request<RpcRequest, RpcWireReply> request;

    public SentRequest(Request<RpcRequest, RpcWireReply> request) {
      this.request = request;
    }

    public void thenPeerWillReply(boolean success) {
      thenPeerWillReply(success, 0);
    }

    public void thenPeerWillReply(boolean success, long myNextIndex) {
      sendAppendEntriesReply(request, success, myNextIndex);
    }

    public long lastEntryIndex() {
      List<LogEntry> entryList = request.getRequest().getAppendMessage().getEntriesList();
      if (entryList.isEmpty()) {
        return 0;
      } else {
        return entryList.get(entryList.size() - 1).getIndex();
      }
    }
  }

  /**
   * Reply true or false to a single AppendEntries request
   */
  private void sendAppendEntriesReply(Request<RpcRequest, RpcWireReply> request,
                                      boolean trueIsSuccessFlag,
                                      long myNextIndex) {
    RpcRequest message = request.getRequest();
    long termFromMessage = message.getAppendMessage().getTerm();
    RpcWireReply reply = new RpcWireReply(message.to, QUORUM_ID,
        new AppendEntriesReply(termFromMessage, trueIsSuccessFlag, myNextIndex));
    request.reply(reply);
  }

  private void replyTo(SentRequest sentRequest, boolean success) {
    replyTo(sentRequest, success, 0);
  }

  private void replyTo(SentRequest sentRequest, boolean success, long myNextLogIndex) {
    sentRequest.thenPeerWillReply(success, myNextLogIndex);
  }

  private void expectLeaderToCommitUpToIndex(long index) {
    commitMonitor.waitFor(hasCommitNoticeIndexValueAtLeast(index));
    assertFalse(commitMonitor.hasAny(hasCommitNoticeIndexValueAtLeast(index + 1)));
  }

}
