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
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.log.ReplicatorLog;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.util.CheckedConsumer;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.CharsetUtil;
import org.hamcrest.Matcher;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static c5db.AsyncChannelAsserts.ChannelHistoryMonitor;
import static c5db.RpcMatchers.RequestMatcher;
import static c5db.RpcMatchers.RequestMatcher.anAppendRequest;
import static c5db.interfaces.replication.Replicator.State.FOLLOWER;
import static c5db.replication.ReplicationMatchers.aNoticeMatchingPeerAndCommitIndex;
import static c5db.replication.ReplicationMatchers.aQuorumChangeCommitNotice;
import static c5db.replication.ReplicationMatchers.hasCommittedEntriesUpTo;
import static c5db.replication.ReplicationMatchers.leaderElectedEvent;
import static c5db.replication.ReplicationMatchers.theLeader;
import static c5db.replication.ReplicationMatchers.willCommitEntriesUpTo;
import static c5db.replication.ReplicationMatchers.willRespondToAnAppendRequest;
import static c5db.replication.ReplicationMatchers.willSend;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

/**
 * A class for tests of the behavior of multiple interacting ReplicatorInstance nodes,
 */
public class InRamTest {
  private static final int TEST_TIMEOUT = 10; // maximum timeout allowed for any election (seconds)
  private static final int ELECTION_TIMEOUT = 100; // milliseconds of lost contact before a follower deposes a leader
  private static final long OFFSET_STAGGERING_MILLIS = 250; // offset between different peers' clocks
  private static final List<Long> INITIAL_PEERS = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L);

  @Rule
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();

  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);
  private final RunnableExecutor runnableExecutor = new RunnableExecutorImpl(batchExecutor);
  private final Fiber fiber = new ThreadFiber(runnableExecutor, "InRamTest-ThreadFiber", true);
  private InRamSim sim;

  private ChannelHistoryMonitor<IndexCommitNotice> commitMonitor;
  private ChannelHistoryMonitor<ReplicatorInstanceEvent> eventMonitor;
  private long lastIndexLogged;

  private final MemoryChannel<Request<RpcRequest, RpcWireReply>> requestLog = new MemoryChannel<>();
  private final ChannelHistoryMonitor<Request<RpcRequest, RpcWireReply>> requestMonitor =
      new ChannelHistoryMonitor<>(requestLog, fiber);

  private final Map<Long, Long> lastCommit = new HashMap<>();

  @Before
  public final void setUpSimulationAndFibers() throws Exception {
    sim = new InRamSim(ELECTION_TIMEOUT, OFFSET_STAGGERING_MILLIS, batchExecutor);

    sim.getRpcChannel().subscribe(fiber, requestLog::publish);
    sim.getCommitNotices().subscribe(fiber, this::updateLastCommit);
    sim.getCommitNotices().subscribe(fiber, System.out::println);
    sim.getStateChanges().subscribe(fiber, System.out::println);
    commitMonitor = new ChannelHistoryMonitor<>(sim.getCommitNotices(), fiber);
    eventMonitor = new ChannelHistoryMonitor<>(sim.getStateChanges(), fiber);

    fiber.start();
    sim.start(INITIAL_PEERS);
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
    allPeers((peer) -> peer.waitForCommit(lastIndexLogged()));

    // Kill the first leader; wait for a second leader to come to power
    leader().die();
    waitForANewLeader();
    assertThat(leader(), willSend(anAppendRequest().withCommitIndex(equalTo(lastIndexLogged()))));
  }

  @Test
  public void aLeaderSendsDataToAllOtherPeersResultingInAllPeersCommitting() throws Throwable {
    havingElectedALeaderAtOrAfter(term(1));
    leader().log(someData());

    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(lastIndexLogged())));
  }

  @Test
  public void aFollowerWillStageANewElectionIfItTimesOutWaitingToHearFromTheLeader() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));

    PeerController follower = pickFollower();

    follower.willDropIncomingAppendsUntil(leader(), is(not(theLeader())));
    follower.allowToTimeout();

    waitForANewLeader();
    assertThat(follower, anyOf(is(theLeader()), willRespondToAnAppendRequest()));
  }

  @Test
  public void ifAFollowerFallsBehindInReceivingAndLoggingEntriesItIsAbleToCatchUp() throws Throwable {
    havingElectedALeaderAtOrAfter(term(1));

    PeerController follower = pickFollower();
    follower.willDropIncomingAppendsUntil(leader(), hasCommittedEntriesUpTo(index(3)));

    leader()
        .log(someData())
        .log(someData());
    assertThat(follower, willCommitEntriesUpTo(index(3)));
  }

  @Test
  public void aReplicatorReturnsNullIfAskedToChangeQuorumsWhenItIsNotInTheLeaderState() throws Exception {
    final List<Long> newPeerIds = Lists.newArrayList(7L, 8L, 9L);
    havingElectedALeaderAtOrAfter(term(1));

    assertThat(pickFollower().changeQuorum(newPeerIds), nullValue());
  }

  @Test(expected = Exception.class)
  public void throwsAnExceptionIfALeaderIsAskedToChangeQuorumsWhileAQuorumChangeIsAlreadyInProgress()
      throws Exception {
    final List<Long> newPeerIds = Lists.newArrayList(7L, 8L, 9L);
    havingElectedALeaderAtOrAfter(term(1));

    assertThat(leader().changeQuorum(newPeerIds), not(nullValue()));

    leader().changeQuorum(newPeerIds); // exception
  }

  @Test
  public void aLeaderCanInitiateAQuorumMembershipChange() throws Throwable {
    final List<Long> newPeerIds = Lists.newArrayList(7L, 8L, 9L);
    final QuorumConfiguration finalConfig = QuorumConfiguration.of(newPeerIds);

    havingElectedALeaderAtOrAfter(term(1));
    leader().changeQuorum(newPeerIds);

    sim.createAndStartReplicators(newPeerIds);
    commitMonitor.waitFor(aQuorumChangeCommitNotice(finalConfig));
    killAllPeersExcept(newPeerIds);

    waitForANewLeader();

    assertThatPeersAreInConfiguration(finalConfig);
    assertThat(newPeerIds, hasItem(equalTo(leader().id)));
  }

  @Test
  public void aQuorumChangeWillGoThroughEvenIfTheLeaderDiesBeforeItCommitsTheTransitionalConfiguration()
      throws Throwable {
    // Leader dies before it can commit the transitional configuration, but as long as the next leader
    // has already received the transitional configuration entry, it can complete the view change.

    final List<Long> newPeerIds = Lists.newArrayList(8L, 9L, 10L);
    final QuorumConfiguration transitionalConfig = QuorumConfiguration.of(INITIAL_PEERS).transitionTo(newPeerIds);

    havingElectedALeaderAtOrAfter(term(1));
    final long nextLogIndex = leader().log.getLastIndex() + 1;
    leader().changeQuorum(newPeerIds);

    // Leader begins distributing the entries containing the transitional config

    assertThat(leader(), willSend(anAppendRequest().containingQuorumConfig(transitionalConfig)));
    allPeersExceptLeader(PeerController::waitForAppendReply);
    assertThat(leader().hasCommittedEntriesUpTo(nextLogIndex), is(false));

    // As of this point, all peers have replicated the transitional config, but the leader has not committed.
    // (It would be impossible to commit because the new peers have not come online, and their votes are
    // necessary to commit the transitional configuration).

    leader().die();
    sim.createAndStartReplicators(newPeerIds);
    waitForANewLeader();
    assertThat(leader().currentConfiguration(), equalTo(transitionalConfig));

    // The new leader cannot commit until it has committed an entry from its current term,
    // so we need to log something for the quorum change to go through.

    leader().log(someData());
    commitMonitor.waitFor(aQuorumChangeCommitNotice(QuorumConfiguration.of(newPeerIds)));
    killAllPeersExcept(newPeerIds);
    assertThatPeersAreInConfiguration(transitionalConfig.completeTransition());
  }

  @Test
  public void afterAQuorumChangeTheNewNodesWillCatchUpToThePreexistingOnes() throws Throwable {
    final List<Long> newPeerIds = Lists.newArrayList(4L, 5L, 6L, 7L, 8L, 9L, 10L);
    final long maximumIndex = 5;
    havingElectedALeaderAtOrAfter(term(1));

    leader().logDataUpToIndex(maximumIndex);
    leader().waitForCommit(maximumIndex);

    sim.createAndStartReplicators(newPeerIds);
    leader().changeQuorum(newPeerIds);

    commitMonitor.waitFor(aQuorumChangeCommitNotice(QuorumConfiguration.of(newPeerIds)));
    killAllPeersExcept(newPeerIds);
    waitForANewLeader();

    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(maximumIndex)));
  }

  /**
   * Private methods
   */

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

  // Blocks until a leader is elected during some term >= minimumTerm.
  // Throws TimeoutException if that does not occur within the time limit.
  private void waitForALeader(long minimumTerm) throws Exception {
    sim.startAllTimeouts();
    eventMonitor.waitFor(leaderElectedEvent(minimumTerm));
    sim.stopAllTimeouts();

    // Wait for at least one other node to recognize the new leader. This is necessary because
    // some tests want to be able to identify a follower right away.
    pickNonLeader().waitForAppendReply();

    final long leaderId = sim.getCurrentLeader();
    assertThat(peer(leaderId), is(theLeader()));
    assertThat(leaderCount(), is(equalTo(1)));

    sim.startTimeout(leaderId);
  }

  private void havingElectedALeaderAtOrAfter(long minimumTerm) throws Exception {
    waitForALeader(minimumTerm);
  }

  private void waitForANewLeader() throws Exception {
    waitForALeader(sim.getCurrentTerm() + 1);
  }

  // Counts leaders in the current term. Used to verify a sane state.
  // If the simulation is running correctly, this should only ever return 0 or 1.
  private int leaderCount() {
    int leaderCount = 0;
    for (ReplicatorInstance replicatorInstance : sim.getReplicators().values()) {
      if (replicatorInstance.isLeader() && replicatorInstance.currentTerm >= sim.getCurrentTerm()) {
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

  private long lastIndexLogged() {
    return lastIndexLogged;
  }

  private PeerController peer(long peerId) {
    return new PeerController(peerId);
  }

  // Syntactic sugar for manipulating leaders

  class LeaderController extends PeerController {
    public LeaderController() {
      super(sim.getCurrentLeader());
    }

    public LeaderController log(List<ByteBuffer> buffers) throws Exception {
      lastIndexLogged = currentLeaderInstance().logData(buffers).get();
      return this;
    }

    public LeaderController logDataUpToIndex(long index) throws Exception {
      while (lastIndexLogged < index) {
        log(someData());
      }
      return this;
    }

    private ReplicatorInstance currentLeaderInstance() {
      return sim.getReplicators().get(sim.getCurrentLeader());
    }
  }

  // Syntactic sugar for manipulating peers
  class PeerController {
    public final long id;
    public final ReplicatorLog log;
    public final ReplicatorInstance instance;

    public PeerController(long id) {
      this.id = id;
      this.log = sim.getLog(id);
      this.instance = sim.getReplicators().get(id);
    }

    public boolean isCurrentLeader() {
      return instance.isLeader()
          && instance.currentTerm >= sim.getCurrentTerm();
    }

    public QuorumConfiguration currentConfiguration() {
      return instance.getQuorumConfiguration();
    }

    public ListenableFuture<Long> changeQuorum(Collection<Long> newPeerIds) throws Exception {
      return instance.changeQuorum(newPeerIds);
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
      commitMonitor.waitFor(aNoticeMatchingPeerAndCommitIndex(id, commitIndex));
      return this;
    }

    public PeerController waitForAppendReply() throws Exception {
      getAppendReplyFuture((reply) -> reply.from == id).get(TEST_TIMEOUT, TimeUnit.SECONDS);
      return this;
    }

    public PeerController waitForRequest(RequestMatcher requestMatcher) {
      requestMonitor.waitFor(requestMatcher.from(id));
      return this;
    }

    public boolean hasCommittedEntriesUpTo(long index) {
      return commitMonitor.hasAny(aNoticeMatchingPeerAndCommitIndex(id, index));
    }

    public void willDropIncomingAppendsUntil(PeerController peer, Matcher<PeerController> matcher) {
      sim.dropIncomingRequests(id,
          (request) -> true,
          (request) -> matcher.matches(peer));
    }
  }

  private void killAllPeersExcept(Collection<Long> peerIds) throws Throwable {
    allPeers((peer) -> {
      if (!peerIds.contains(peer.id)) {
        sim.killPeer(peer.id);
      }
    });
  }

  private void allPeers(CheckedConsumer<PeerController, Throwable> forEach) throws Throwable {
    for (long peerId : sim.getOnlinePeers()) {
      forEach.accept(new PeerController(peerId));
    }
  }

  private void allPeersExceptLeader(CheckedConsumer<PeerController, Throwable> forEach) throws Throwable {
    for (long peerId : sim.getOnlinePeers()) {
      if (peerId == sim.getCurrentLeader()) {
        continue;
      }
      forEach.accept(new PeerController(peerId));
    }
  }

  private PeerController anyPeerSuchThat(Predicate<PeerController> predicate) {
    for (long peerId : sim.getOnlinePeers()) {
      if (predicate.test(peer(peerId))) {
        return peer(peerId);
      }
    }
    return null;
  }

  private void assertThatPeersAreInConfiguration(QuorumConfiguration quorumConfiguration) {
    for (long peerId : sim.getOnlinePeers()) {
      assertThat(peer(peerId).currentConfiguration(), equalTo(quorumConfiguration));
    }
  }

  private PeerController pickFollower() {
    PeerController chosenPeer = anyPeerSuchThat((peer) -> peer.instance.myState == FOLLOWER && peer.isOnline());
    assertThat(chosenPeer, not(nullValue()));
    return chosenPeer;
  }

  private PeerController pickNonLeader() {
    PeerController chosenPeer = anyPeerSuchThat((peer) -> not(theLeader()).matches(peer) && peer.isOnline());
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
}
