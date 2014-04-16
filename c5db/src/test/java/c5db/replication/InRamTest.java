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

import c5db.AsyncChannelAsserts;
import c5db.log.ReplicatorLog;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.RequestVote;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.util.CheckedConsumer;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.CharsetUtil;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Request;
import org.jetlang.core.BatchExecutor;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static c5db.interfaces.ReplicationModule.IndexCommitNotice;
import static c5db.interfaces.ReplicationModule.Replicator.State.FOLLOWER;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNot.not;

/**
 * A class for tests of the behavior of multiple interacting ReplicatorInstance nodes,
 */
public class InRamTest {
  private static final int ELECTION_LIMIT = 20;  // maximum number of election attempts
  private static final int TEST_TIMEOUT = 10; // maximum timeout allowed for any election (seconds)
  private static final int NUM_PEERS = 7; // number of nodes in simulations; needs to be > 1
  private static final int ELECTION_TIMEOUT = 100; // milliseconds of lost contact before a follower deposes a leader
  private static final long OFFSET_STAGGERING_MILLIS = 250; // offset between different peers' clocks

  @Rule
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();

  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);
  private final RunnableExecutor runnableExecutor = new RunnableExecutorImpl(batchExecutor);
  private final Fiber fiber = new ThreadFiber(runnableExecutor, null, true);
  private InRamSim sim;
  private final Channel<Long> simStatusChanges = new MemoryChannel<>();

  private long currentTerm = 0;
  private long currentLeader = 0;

  private AsyncChannelAsserts.ChannelHistoryMonitor<IndexCommitNotice> commitMonitor;

  private final Map<Long, Long> lastCommit = new HashMap<>();

  @Before
  public final void setUpSimulationAndFibers() {
    assertThat(NUM_PEERS, is(greaterThan(1)));
    sim = new InRamSim(NUM_PEERS, ELECTION_TIMEOUT, OFFSET_STAGGERING_MILLIS, batchExecutor);

    sim.getRpcChannel().subscribe(fiber, this::monitorOutboundRequests);
    sim.getCommitNotices().subscribe(fiber, this::updateLastCommit);
    commitMonitor = new AsyncChannelAsserts.ChannelHistoryMonitor<>(sim.getCommitNotices(), fiber);

    sim.start();
    fiber.start();
  }

  @After
  public final void disposeResources() {
    sim.dispose();
    fiber.dispose();
  }

  @Test
  public void aLeaderWillBeElectedInATimelyMannerInANewQuorum() throws Exception {
    waitForALeader(term(1));
  }

  @Test
  public void aNewLeaderWillBeElectedIfAnExistingLeaderDies() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));

    leader().die();
    waitForANewLeader();
  }

  @Test
  public void ifAKilledLeaderIsRestartedItWillBecomeAFollower() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));

    LeaderController firstLeader = leader();
    firstLeader.die();

    waitForANewLeader();
    assertThat(leader(), is(not(equalTo(firstLeader))));

    firstLeader.restart();
    assertThat(firstLeader, willRespondToAnAppendRequest());
  }

  @Test
  public void ifAnElectionOccursWhileAPeerIsOfflineThenThePeerWillRecognizeTheNewLeaderWhenThePeerRestarts()
      throws Throwable {
    havingElectedALeaderAtOrAfter(term(1));

    LeaderController firstLeader = leader();
    PeerController follower = pickFollower().die();

    leader().log(someData())
        .waitForCommit(index(1))
        .die();

    waitForANewLeader();
    assertThat(leader(), not(equalTo(firstLeader)));

    // The second leader logs some entry, then the first node to go offline comes back
    leader().log(someData());
    follower.restart()
        .waitForCommit(index(2));

    leader().log(someData());
    firstLeader.restart();

    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(index(3))));
  }

  @Test
  public void aFollowerMaintainsItsCommitIndexWhenItBecomesLeader() throws Throwable {
    havingElectedALeaderAtOrAfter(term(1));

    leader().log(someData());
    allPeers((peer) -> peer.waitForCommit(index(1)));

    // Kill the first leader; wait for a second leader to come to power
    leader().die();
    waitForANewLeader();
    assertThat(leader(), willSendAnAppendRequestWithCommitIndex(equalTo(index(1))));
  }

  @Test
  public void aLeaderSendsDataToAllOtherPeersResultingInAllPeersCommitting() throws Throwable {
    havingElectedALeaderAtOrAfter(term(1));
    leader().log(someData());

    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(index(1))));
  }

  @Test
  public void aFollowerWillStageANewElectionIfItTimesOutWaitingToHearFromTheLeader() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));

    PeerController follower = pickFollower();

    follower.willDropIncomingAppendsUntil(leader(), is(not(aLeader())));
    follower.allowToTimeout();

    waitForANewLeader();
    assertThat(follower, anyOf(is(aLeader()), willRespondToAnAppendRequest()));
  }

  @Test
  public void ifAFollowerFallsBehindInReceivingAndLoggingEntriesItIsAbleToCatchUp() throws Throwable {
    havingElectedALeaderAtOrAfter(term(1));

    PeerController follower = pickFollower();
    follower.willDropIncomingAppendsUntil(leader(), hasCommittedEntriesUpTo(index(2)));

    leader()
        .log(someData())
        .log(someData());
    assertThat(follower, willCommitEntriesUpTo(index(2)));
  }

  /**
   * Private methods
   */

  // Observe outbound requests made by peers, and use them to infer state (currentTerm, currentLeader)
  private void monitorOutboundRequests(Request<RpcRequest, RpcWireReply> request) {
    RequestVote requestVoteMessage = request.getRequest().getRequestVoteMessage();
    AppendEntries appendEntriesMessage = request.getRequest().getAppendMessage();
    long term = 0;
    boolean statusChange = false;

    if (requestVoteMessage != null) {
      term = requestVoteMessage.getTerm();
      if (term > currentTerm) {
        // A new voting phase has begun
        currentLeader = 0;
        statusChange = true;
      }
    } else if (appendEntriesMessage != null) {
      term = appendEntriesMessage.getTerm();
      if (term >= currentTerm && currentLeader == 0) {
        currentLeader = request.getRequest().from;
        statusChange = true;
      }
    }

    if (term > currentTerm) {
      currentTerm = term;
      statusChange = true;
    }

    if (statusChange) {
      simStatusChanges.publish(currentTerm);
    }
  }

  // Return a future to signal when a leader has been chosen. The caller must specify the minimum term number
  // in which the election "counts"; otherwise it is not well-determined when a leader is chosen. The future
  // will be set to true if a leader is successfully chosen before 'ELECTION_LIMIT' terms elapse; otherwise
  // the future will be set to false.
  private ListenableFuture<Boolean> getLeaderChosenFuture(long minimumTerm) {
    SettableFuture<Boolean> future = SettableFuture.create();
    if (currentLeader != 0 && currentTerm >= minimumTerm) {
      future.set(true);
    } else {
      long maximumTerm = currentTerm + ELECTION_LIMIT;
      simStatusChanges.subscribe(fiber, (term) -> {
        if (currentTerm > maximumTerm) {
          future.set(false);
        } else if (currentLeader != 0 && currentTerm >= minimumTerm) {
          future.set(true);
        }
      });
    }
    return future;
  }

  // Return a future to enable waiting for a follower to reply to an Append request. Only replies which satisfy
  // the predicate isValid are considered. For instance, isValid could restrict to a specific peerId. The purpose
  // is to check the "liveness" of a peer other than the leader (a leader's liveness is considered irrelevant once
  // enough time elapses that at least one follower initiates a RequestVote. Nothing similar occurs if a follower
  // stops responding).
  private ListenableFuture<Boolean> getAppendReplyFuture(Predicate<RpcWireReply> isValid) {
    SettableFuture<Boolean> future = SettableFuture.create();
    sim.getReplyChannel().subscribe(fiber, (reply) -> {
      if (reply.getAppendReplyMessage() != null && isValid.test(reply)) {
        future.set(true);
      }
    });
    return future;
  }

  private ListenableFuture<Boolean> getAppendRequestFuture(Predicate<RpcRequest> isValid) {
    SettableFuture<Boolean> future = SettableFuture.create();
    sim.getRpcChannel().subscribe(fiber, (request) -> {
      if (request.getRequest().getAppendMessage() != null
          && isValid.test(request.getRequest())) {
        future.set(true);
      }
    });
    return future;
  }

  // Blocks until a leader is elected during some term >= minimumTerm.
  // Throws TimeoutException if that does not occur within the time limit.
  private void waitForALeader(long minimumTerm) throws Exception {
    sim.startAllTimeouts();
    getLeaderChosenFuture(minimumTerm).get(TEST_TIMEOUT, TimeUnit.SECONDS);
    sim.stopAllTimeouts();

    // Wait for at least one other node to recognize the new leader. This is necessary because
    // some tests want to be able to identify a follower right away.
    pickNonLeader().waitForAppendReply();

    assertThat(peer(currentLeader), is(aLeader()));
    assertThat(leaderCount(), is(equalTo(1)));

    sim.startTimeout(currentLeader);
  }

  private void havingElectedALeaderAtOrAfter(long minimumTerm) throws Exception {
    waitForALeader(minimumTerm);
  }

  private void waitForANewLeader() throws Exception {
    waitForALeader(currentTerm + 1);
  }

  // Counts leaders in the current term. Used to verify a sane state.
  // If the simulation is running correctly, this should only ever return 0 or 1.
  private int leaderCount() {
    int leaderCount = 0;
    for (ReplicatorInstance repl : sim.getReplicators().values()) {
      if (repl.isLeader() && repl.currentTerm >= this.currentTerm) {
        leaderCount++;
      }
    }
    return leaderCount;
  }

  private static List<ByteBuffer> someData() {
    return Lists.newArrayList(
        ByteBuffer.wrap("test".getBytes(CharsetUtil.UTF_8)));
  }

  private LeaderController leader() {
    return new LeaderController();
  }

  private PeerController peer(long peerId) {
    return new PeerController(peerId);
  }

  // Syntactic sugar for manipulating leaders
  private class LeaderController extends PeerController {
    private ReplicatorInstance currentLeader() {
      return sim.getReplicators().get(currentLeader);
    }

    public LeaderController() {
      super(currentLeader);
    }

    public LeaderController log(List<ByteBuffer> buffers) throws InterruptedException {
      currentLeader().logData(buffers);
      return this;
    }
  }

  // Syntactic sugar for manipulating peers
  private class PeerController {
    public final long id;
    public final ReplicatorLog log;
    public final ReplicatorInstance instance;

    public PeerController(long id) {
      this.id = id;
      this.log = sim.getLog(id);
      this.instance = sim.getReplicators().get(id);
    }

    public PeerController die() {
      sim.killPeer(id);
      return this;
    }

    public PeerController restart() {
      sim.restartPeer(id);
      return this;
    }

    public boolean isOnline() {
      return !sim.getOfflinePeers().contains(id);
    }

    public PeerController allowToTimeout() {
      sim.startTimeout(id);
      return this;
    }

    public PeerController waitForCommit(long commitIndex) {
      commitMonitor.waitFor(matchPeerAndCommitIndex(id, commitIndex));
      return this;
    }

    public PeerController waitForAppendReply() throws Exception {
      getAppendReplyFuture((reply) -> reply.from == id).get(TEST_TIMEOUT, TimeUnit.SECONDS);
      return this;
    }

    public void willDropIncomingAppendsUntil(PeerController peer, Matcher<PeerController> matcher) {
      sim.dropIncomingRequests(id,
          (request) -> true,
          (request) -> matcher.matches(peer));
    }
  }

  private void allPeers(CheckedConsumer<PeerController, Throwable> forEach) throws Throwable {
    for (long peerId : sim.peerIds) {
      forEach.accept(new PeerController(peerId));
    }
  }

  private PeerController anyPeerSuchThat(Predicate<PeerController> predicate) {
    for (long peerId : sim.peerIds) {
      if (predicate.test(peer(peerId))) {
        return peer(peerId);
      }
    }
    return null;
  }

  private PeerController pickFollower() {
    PeerController chosenPeer = anyPeerSuchThat((peer) -> peer.instance.myState == FOLLOWER && peer.isOnline());
    assertThat(chosenPeer, not(nullValue()));
    return chosenPeer;
  }

  private PeerController pickNonLeader() {
    PeerController chosenPeer = anyPeerSuchThat((peer) -> not(aLeader()).matches(peer) && peer.isOnline());
    assertThat(chosenPeer, not(nullValue()));
    return chosenPeer;
  }

  private static long term(long term) {
    return term;
  }

  private static long index(long index) {
    return index;
  }

  private void updateLastCommit(IndexCommitNotice notice) {
    long peerId = notice.replicatorInstance.getId();
    if (notice.committedIndex > lastCommit.getOrDefault(peerId, 0L)) {
      lastCommit.put(peerId, notice.committedIndex);
    }
  }

  /**
   * Matchers
   */

  private static Matcher<IndexCommitNotice> matchPeerAndCommitIndex(long peerId, long index) {
    return new TypeSafeMatcher<IndexCommitNotice>() {
      @Override
      protected boolean matchesSafely(IndexCommitNotice item) {
        return item.committedIndex >= index &&
            item.replicatorInstance.getId() == peerId;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a commit index at least ")
            .appendValue(index)
            .appendText(" for peer ")
            .appendValue(peerId);
      }
    };
  }

  Matcher<PeerController> hasCommittedEntriesUpTo(long index) {
    return new TypeSafeMatcher<PeerController>() {
      @Override
      protected boolean matchesSafely(PeerController peer) {
        return commitMonitor.hasAny(matchPeerAndCommitIndex(peer.id, index));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer has committed log entries up to index" + index);
      }
    };
  }

  // TODO many of the following matchers have similar structure that should probably be factored out.

  Matcher<PeerController> willCommitEntriesUpTo(long index) {
    return new TypeSafeMatcher<PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(PeerController peer) {
        try {
          peer.waitForCommit(index);
          assert peer.log.getLastIndex() >= index;
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will commit log entries up to index " + index);
      }

      @Override
      public void describeMismatchSafely(PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }

  Matcher<PeerController> aLeader() {
    return new TypeSafeMatcher<PeerController>() {
      @Override
      protected boolean matchesSafely(PeerController peer) {
        return peer.instance.isLeader()
            && peer.instance.currentTerm >= currentTerm;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("A peer in LEADER state");
      }
    };
  }

  Matcher<PeerController> willRespondToAnAppendRequest() {
    return new TypeSafeMatcher<PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(PeerController peer) {
        try {
          peer.waitForAppendReply();
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will respond to an AppendEntries request");
      }

      @Override
      public void describeMismatchSafely(PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }

  Matcher<PeerController> willSendAnAppendRequestWithCommitIndex(Matcher<Long> indexMatcher) {
    return new TypeSafeMatcher<PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(PeerController peer) {
        try {
          getAppendRequestFuture((request) ->
              request.from == peer.id
                  && indexMatcher.matches(request.getAppendMessage().getCommitIndex()))
              .get(TEST_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will respond to an AppendEntries request");
      }

      @Override
      public void describeMismatchSafely(PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }
}
