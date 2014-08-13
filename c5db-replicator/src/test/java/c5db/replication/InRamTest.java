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
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.interfaces.replication.ReplicatorReceipt;
import c5db.replication.rpc.RpcMessage;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.util.CheckedConsumer;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static c5db.AsyncChannelAsserts.ChannelHistoryMonitor;
import static c5db.CollectionMatchers.isIn;
import static c5db.FutureMatchers.resultsIn;
import static c5db.IndexCommitMatcher.aCommitNotice;
import static c5db.RpcMatchers.ReplyMatcher.aPreElectionReply;
import static c5db.RpcMatchers.ReplyMatcher.anAppendReply;
import static c5db.RpcMatchers.RequestMatcher;
import static c5db.RpcMatchers.RequestMatcher.aPreElectionPoll;
import static c5db.RpcMatchers.RequestMatcher.anAppendRequest;
import static c5db.RpcMatchers.containsQuorumConfiguration;
import static c5db.interfaces.replication.Replicator.State.FOLLOWER;
import static c5db.interfaces.replication.ReplicatorInstanceEvent.EventType.ELECTION_TIMEOUT;
import static c5db.replication.ReplicationMatchers.aQuorumChangeCommittedEvent;
import static c5db.replication.ReplicationMatchers.aReplicatorEvent;
import static c5db.replication.ReplicationMatchers.hasCommittedEntriesUpTo;
import static c5db.replication.ReplicationMatchers.leaderElectedEvent;
import static c5db.replication.ReplicationMatchers.theLeader;
import static c5db.replication.ReplicationMatchers.willCommitConfiguration;
import static c5db.replication.ReplicationMatchers.willCommitEntriesUpTo;
import static c5db.replication.ReplicationMatchers.willRespondToAnAppendRequest;
import static c5db.replication.ReplicationMatchers.willSend;
import static c5db.replication.ReplicationMatchers.wonAnElectionWithTerm;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.IsNot.not;

/**
 * A class for tests of the behavior of multiple interacting ReplicatorInstance nodes,
 */
public class InRamTest {
  private static final int ELECTION_TIMEOUT_MILLIS = 50; // election timeout (milliseconds)
  private static final long OFFSET_STAGGERING_MILLIS = 50; // offset between different peers' clocks

  @Rule
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();

  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);
  private final RunnableExecutor runnableExecutor = new RunnableExecutorImpl(batchExecutor);
  private final Fiber fiber = new ThreadFiber(runnableExecutor, "InRamTest-ThreadFiber", true);
  private InRamSim sim;

  private ChannelHistoryMonitor<IndexCommitNotice> commitMonitor;
  private ChannelHistoryMonitor<ReplicatorInstanceEvent> eventMonitor;
  private ChannelHistoryMonitor<RpcMessage> replyMonitor;
  private long lastIndexLogged;

  private final MemoryChannel<Request<RpcRequest, RpcWireReply>> requestLog = new MemoryChannel<>();
  private final ChannelHistoryMonitor<Request<RpcRequest, RpcWireReply>> requestMonitor =
      new ChannelHistoryMonitor<>(requestLog, fiber);

  private final Map<Long, Long> lastCommit = new HashMap<>();

  @Before
  public final void setUpSimulationAndFibers() throws Exception {
    sim = new InRamSim(ELECTION_TIMEOUT_MILLIS, OFFSET_STAGGERING_MILLIS, batchExecutor);

    sim.getRpcChannel().subscribe(fiber, requestLog::publish);
    sim.getCommitNotices().subscribe(fiber, this::updateLastCommit);
    sim.getCommitNotices().subscribe(fiber, System.out::println);
    sim.getEventChannel().subscribe(fiber, System.out::println);
    commitMonitor = new ChannelHistoryMonitor<>(sim.getCommitNotices(), fiber);
    eventMonitor = new ChannelHistoryMonitor<>(sim.getEventChannel(), fiber);
    replyMonitor = new ChannelHistoryMonitor<>(sim.getReplyChannel(), fiber);

    fiber.start();
    sim.start(initialPeerSet());
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
    assertThat(firstLeader, willRespondToAnAppendRequest(currentTerm()));
  }

  @Test
  public void ifAnElectionOccursWhileAPeerIsOfflineThenThePeerWillRecognizeTheNewLeaderWhenThePeerRestarts()
      throws Exception {
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
  public void aFollowerMaintainsItsCommitIndexWhenItBecomesLeader() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));

    leader().log(someData());
    allPeers((peer) -> peer.waitForCommit(lastIndexLogged()));

    // Kill the first leader; wait for a second leader to come to power
    leader().die();
    waitForANewLeader();
    leader().log(someData());

    assertThat(leader(), willSend(anAppendRequest().withCommitIndex(equalTo(lastIndexLogged()))));
  }

  @Test
  public void aLeaderSendsDataToAllOtherPeersResultingInAllPeersCommitting() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));
    leader().log(someData());

    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(lastIndexLogged())));
  }

  @Test
  public void aFollowerWillStageANewElectionIfItTimesOutWaitingToHearFromTheLeader() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));
    final long firstLeaderTerm = currentTerm();

    PeerController follower = pickFollower();

    follower.willDropIncomingAppendsUntil(leader(), is(not(theLeader())));
    follower.allowToTimeout();

    waitForALeader(firstLeaderTerm + 1);
    assertThat(follower, anyOf(is(theLeader()), willRespondToAnAppendRequest(currentTerm())));
  }

  @Test
  public void ifAFollowerFallsBehindInReceivingAndLoggingEntriesItIsAbleToCatchUp() throws Exception {
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
    final Set<Long> newPeerIds = smallerPeerSetWithOneInCommonWithInitialSet();
    havingElectedALeaderAtOrAfter(term(1));

    assertThat(pickFollower().changeQuorum(newPeerIds), nullValue());
  }

  @Test
  public void aLeaderCanCoordinateAQuorumMembershipChange() throws Exception {
    final Set<Long> newPeerIds = smallerPeerSetWithNoneInCommonWithInitialSet();
    final QuorumConfiguration finalConfig = QuorumConfiguration.of(newPeerIds);

    havingElectedALeaderAtOrAfter(term(1));

    leader().changeQuorum(newPeerIds);
    sim.createAndStartReplicators(newPeerIds);

    waitForANewLeader();
    leader().log(someData());

    peers(newPeerIds).forEach((peer) ->
        assertThat(peer, willCommitConfiguration(finalConfig)));
    assertThat(newPeerIds, hasItem(equalTo(leader().id)));
  }

  @Test
  public void aSecondQuorumChangeWillOverrideTheFirst() throws Exception {
    final Set<Long> firstPeerSet = smallerPeerSetWithOneInCommonWithInitialSet();
    final Set<Long> secondPeerSet = largerPeerSetWithSomeInCommonWithInitialSet();

    havingElectedALeaderAtOrAfter(term(1));

    leader().changeQuorum(firstPeerSet);
    sim.createAndStartReplicators(firstPeerSet);

    leader().changeQuorum(secondPeerSet);
    sim.createAndStartReplicators(secondPeerSet);

    waitForALeaderWithId(isIn(secondPeerSet));
    leader().log(someData());

    peers(secondPeerSet).forEach((peer) ->
        assertThat(peer,
            willCommitConfiguration(
                QuorumConfiguration.of(secondPeerSet))));
  }

  @Test
  public void theFutureReturnedByAQuorumChangeRequestWillReturnTheReceiptOfTheTransitionalConfigurationEntry()
      throws Exception {
    final Set<Long> newPeerIds = smallerPeerSetWithOneInCommonWithInitialSet();
    final long lastIndexBeforeQuorumChange = 4;

    havingElectedALeaderAtOrAfter(term(1));
    final long electionTerm = currentTerm();

    sim.createAndStartReplicators(newPeerIds);
    leader().logDataUpToIndex(lastIndexBeforeQuorumChange);

    assertThat(leader().changeQuorum(newPeerIds),
        resultsIn(equalTo(
            new ReplicatorReceipt(electionTerm, lastIndexBeforeQuorumChange + 1))));
  }

  @Test
  public void aQuorumChangeWillGoThroughEvenIfTheLeaderDiesBeforeItCommitsTheTransitionalConfiguration()
      throws Exception {
    // Leader dies before it can commit the transitional configuration, but as long as the next leader
    // has already received the transitional configuration entry, it can complete the view change.

    final Set<Long> newPeerIds = smallerPeerSetWithNoneInCommonWithInitialSet();
    final QuorumConfiguration transitionalConfig =
        QuorumConfiguration.of(initialPeerSet()).getTransitionalConfiguration(newPeerIds);

    havingElectedALeaderAtOrAfter(term(1));
    final long nextLogIndex = leader().log.getLastIndex() + 1;
    leader().changeQuorum(newPeerIds);

    allPeersExceptLeader((peer) ->
        assertThat(leader(), willSend(
            anAppendRequest()
                .containingQuorumConfig(transitionalConfig)
                .to(peer.id))));

    ignoringPreviousReplies();
    allPeers((peer) -> peer.waitForAppendReply(greaterThanOrEqualTo(currentTerm())));
    assertThat(leader().hasCommittedEntriesUpTo(nextLogIndex), is(false));

    // As of this point, all peers have replicated the transitional config, but the leader has not committed.
    // It would be impossible to commit because the new peers have not come online, and their votes are
    // necessary to commit the transitional configuration.

    leader().die();
    sim.createAndStartReplicators(newPeerIds);
    waitForANewLeader();
    assertThat(leader().currentConfiguration(), equalTo(transitionalConfig));

    // Necessary to log again because the new leader may not commit an entry from a past term (such as
    // the configuration entry) until it has also committed an entry from its current term.
    leader().log(someData());

    peers(newPeerIds).forEach((peer) ->
        assertThat(peer, willCommitConfiguration(QuorumConfiguration.of(newPeerIds))));
  }

  @Test
  public void afterAQuorumChangeTheNewNodesWillCatchUpToThePreexistingOnes() throws Exception {
    final Set<Long> newPeerIds = largerPeerSetWithSomeInCommonWithInitialSet();
    final long maximumIndex = 5;

    havingElectedALeaderAtOrAfter(term(1));

    leader()
        .logDataUpToIndex(maximumIndex)
        .waitForCommit(maximumIndex);

    sim.createAndStartReplicators(newPeerIds);
    leader().changeQuorum(newPeerIds);

    peers(newPeerIds).forEach((peer) -> {
      assertThat(peer, willCommitEntriesUpTo(maximumIndex));
      assertThat(peer, willCommitConfiguration(QuorumConfiguration.of(newPeerIds)));
    });
  }

  @Test
  public void aQuorumCanMakeProgressEvenIfAFollowerCanSendRequestsButNotReceiveReplies() throws Exception {
    final long maximumIndex = 5;
    havingElectedALeaderAtOrAfter(term(1));

    pickFollower()
        .willDropAllIncomingTraffic()
        .allowToTimeout();

    waitForAnElectionTimeout();

    leader().logDataUpToIndex(maximumIndex);
    assertThat(leader(), willCommitEntriesUpTo(maximumIndex));
  }

  @Test
  public void aQuorumChangeCanCompleteEvenIfARemovedPeerTimesOutDuringIt() throws Exception {
    final Set<Long> newPeerIds = smallerPeerSetWithNoneInCommonWithInitialSet();
    final QuorumConfiguration transitionalConfig =
        QuorumConfiguration.of(initialPeerSet()).getTransitionalConfiguration(newPeerIds);
    final QuorumConfiguration finalConfig = transitionalConfig.getCompletedConfiguration();

    havingElectedALeaderAtOrAfter(term(1));
    final long firstLeaderTerm = currentTerm();
    final long leaderId = currentLeader();

    dropAllAppendsWithThisConfigurationUntilAPreElectionPollTakesPlace(finalConfig);

    leader().changeQuorum(newPeerIds);
    sim.createAndStartReplicators(newPeerIds);

    allPeersExceptLeader((peer) ->
        assertThat(leader(), willSend(
            anAppendRequest()
                .containingQuorumConfig(transitionalConfig)
                .to(peer.id))));

    peers(newPeerIds).forEach((peer) ->
        assertThat(peer, willCommitConfiguration(transitionalConfig)));

    peersBeingRemoved(transitionalConfig).forEach(PeerController::allowToTimeout);
    waitForAnElectionTimeout();

    peersBeingRemoved(transitionalConfig).forEach((peer) -> {
      if (peer.id != leaderId) {
        assertThat(peer, willSend(aPreElectionPoll()));
      }
    });

    waitForALeaderWithId(isIn(newPeerIds));
    leader().log(someData());

    peers(newPeerIds).forEach((peer) ->
        assertThat(peer, willCommitConfiguration(finalConfig)));

    peersBeingRemoved(transitionalConfig).forEach((peer) ->
        assertThat(peer, not(wonAnElectionWithTerm(greaterThan(firstLeaderTerm)))));
  }

  @Test
  public void aQuorumCanElectANewLeaderEvenWhileReceivingMessagesFromRemovedPeersWhoHaveTimedOut() throws Exception {
    final Set<Long> newPeerIds = smallerPeerSetWithNoneInCommonWithInitialSet();
    final QuorumConfiguration transitionalConfig =
        QuorumConfiguration.of(initialPeerSet()).getTransitionalConfiguration(newPeerIds);
    final QuorumConfiguration finalConfig = transitionalConfig.getCompletedConfiguration();

    havingElectedALeaderAtOrAfter(term(1));
    long firstLeaderTerm = currentTerm();

    leader().changeQuorum(newPeerIds);
    sim.createAndStartReplicators(newPeerIds);

    peers(newPeerIds).forEach((peer) ->
        assertThat(peer, willCommitConfiguration(finalConfig)));

    peersBeingRemoved(transitionalConfig).forEach(PeerController::allowToTimeout);
    waitForAnElectionTimeout();

    waitForALeader(term(firstLeaderTerm + 1));

    leader().die();

    waitForANewLeader();
  }

  @Test
  public void aLateBootstrapCallWillBeDisregarded() throws Exception {
    havingElectedALeaderAtOrAfter(term(1));

    leader().logDataUpToIndex(2);
    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(lastIndexLogged())));

    // Bootstrap calls to both leader and a non-leader -- both will be no-ops
    pickNonLeader().instance.bootstrapQuorum(smallerPeerSetWithNoneInCommonWithInitialSet());
    leader().instance.bootstrapQuorum(smallerPeerSetWithNoneInCommonWithInitialSet());

    // Verify that quorum is still in a working state
    assertThat(sim.getLog(leader().id).getLastIndex(), is(equalTo(2L)));

    leader().logDataUpToIndex(3);
    allPeers((peer) -> assertThat(peer, willCommitEntriesUpTo(lastIndexLogged())));
  }

  /**
   * Private methods
   */

  // Blocks until a leader is elected during some term >= minimumTerm.
  // Throws TimeoutException if that does not occur within the time limit.
  private void waitForALeader(long minimumTerm) {
    waitForALeaderElectedEventMatching(anyLeader(), greaterThanOrEqualTo(minimumTerm));
  }

  private void waitForALeaderWithId(Matcher<Long> leaderIdMatcher) {
    waitForALeaderElectedEventMatching(leaderIdMatcher, anyTerm());
  }

  private void waitForALeaderElectedEventMatching(Matcher<Long> leaderIdMatcher, Matcher<Long> termMatcher) {
    sim.startAllTimeouts();
    eventMonitor.waitFor(leaderElectedEvent(leaderIdMatcher, termMatcher));
    sim.stopAllTimeouts();

    // Wait for at least one other node to recognize the new leader. This is necessary because
    // some tests want to be able to identify a follower right away.
    pickNonLeader().waitForAppendReply(termMatcher);

    final long leaderId = currentLeader();
    assertThat(leaderCount(), is(equalTo(1)));

    sim.startTimeout(leaderId);
  }

  private void havingElectedALeaderAtOrAfter(long minimumTerm) {
    waitForALeader(minimumTerm);
  }

  private void waitForANewLeader() {
    waitForALeader(currentTerm() + 1);
  }

  // Counts leaders in the current term. Used to verify a sensible state.
  // If the simulation is running correctly, this should only ever return 0 or 1.
  private int leaderCount() {
    final long currentTerm = currentTerm();
    int leaderCount = 0;
    for (ReplicatorInstance replicatorInstance : sim.getReplicators().values()) {
      if (replicatorInstance.isLeader() && replicatorInstance.currentTerm >= currentTerm) {
        leaderCount++;
      }
    }
    return leaderCount;
  }

  private void waitForAnElectionTimeout() {
    eventMonitor.waitFor(aReplicatorEvent(ELECTION_TIMEOUT));
  }

  private void ignoringPreviousReplies() {
    replyMonitor.forgetHistory();
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

  // Syntactic sugar for manipulating leaders

  class LeaderController extends PeerController {
    public LeaderController() {
      super(currentLeader());
    }

    public LeaderController log(List<ByteBuffer> buffers) throws Exception {
      lastIndexLogged = currentLeaderInstance().logData(buffers).get().seqNum;
      return this;
    }

    public LeaderController logDataUpToIndex(long index) throws Exception {
      while (lastIndexLogged < index) {
        log(someData());
      }
      return this;
    }

    private ReplicatorInstance currentLeaderInstance() {
      return sim.getReplicators().get(currentLeader());
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

    @Override
    public String toString() {
      return instance.toString();
    }

    public boolean isCurrentLeader() {
      return instance.isLeader()
          && instance.currentTerm >= currentTerm();
    }

    public QuorumConfiguration currentConfiguration() {
      return instance.getQuorumConfiguration();
    }

    public ListenableFuture<ReplicatorReceipt> changeQuorum(Collection<Long> newPeerIds) throws Exception {
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
      commitMonitor.waitFor(aCommitNotice().withIndex(greaterThanOrEqualTo(commitIndex)).issuedFromPeer(id));
      return this;
    }

    public PeerController waitForQuorumCommit(QuorumConfiguration quorumConfiguration) {
      eventMonitor.waitFor(aQuorumChangeCommittedEvent(quorumConfiguration, equalTo(id)));
      return this;
    }

    public PeerController waitForAppendReply(Matcher<Long> termMatcher) {
      replyMonitor.waitFor(anAppendReply().withTerm(termMatcher));
      return this;
    }

    public PeerController waitForRequest(RequestMatcher requestMatcher) {
      requestMonitor.waitFor(requestMatcher.from(id));
      return this;
    }

    public boolean hasCommittedEntriesUpTo(long index) {
      return commitMonitor.hasAny(aCommitNotice().withIndex(greaterThanOrEqualTo(index)).issuedFromPeer(id));
    }

    public boolean hasWonAnElection(Matcher<Long> termMatcher) {
      return eventMonitor.hasAny(leaderElectedEvent(equalTo(id), termMatcher));
    }

    public void willDropIncomingAppendsUntil(PeerController peer, Matcher<PeerController> matcher) {
      sim.dropMessages(
          (message) -> message.to == id && message.isAppendMessage(),
          (message) -> matcher.matches(peer));
    }

    public PeerController willDropAllIncomingTraffic() {
      sim.dropMessages(
          (message) -> (message.to == id) && (message.to != message.from),
          (message) -> false);
      return this;
    }
  }


  private PeerController peer(long peerId) {
    return new PeerController(peerId);
  }

  private Set<PeerController> peers(Collection<Long> peerIds) {
    return peerIds.stream()
        .map(this::peer)
        .collect(Collectors.toSet());
  }

  private <Ex extends Throwable> void allPeers(CheckedConsumer<PeerController, Ex> forEach) throws Ex {
    for (long peerId : sim.getOnlinePeers()) {
      forEach.accept(new PeerController(peerId));
    }
  }

  private <Ex extends Throwable> void allPeersExceptLeader(CheckedConsumer<PeerController, Ex> forEach) throws Ex {
    for (long peerId : sim.getOnlinePeers()) {
      if (peerId == currentLeader()) {
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

  private Set<PeerController> peersBeingRemoved(QuorumConfiguration configuration) {
    assert configuration.isTransitional;

    return Sets.difference(configuration.prevPeers(), configuration.nextPeers())
        .stream()
        .map(PeerController::new)
        .collect(Collectors.toSet());
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

  private long currentTerm() {
    return eventMonitor.getLatest(leaderElectedEvent(anyLeader(), anyTerm())).leaderElectedTerm;
  }

  private long currentLeader() {
    return eventMonitor.getLatest(leaderElectedEvent(anyLeader(), anyTerm())).newLeader;
  }

  private static long term(long term) {
    return term;
  }

  private static long index(long index) {
    return index;
  }

  private static Matcher<Long> anyLeader() {
    return any(Long.class);
  }

  private static Matcher<Long> anyTerm() {
    return any(Long.class);
  }

  private void updateLastCommit(IndexCommitNotice notice) {
    long peerId = notice.nodeId;
    if (notice.lastIndex > lastCommit.getOrDefault(peerId, 0L)) {
      lastCommit.put(peerId, notice.lastIndex);
    }
  }

  private void dropAllAppendsWithThisConfigurationUntilAPreElectionPollTakesPlace(QuorumConfiguration configuration) {
    sim.dropMessages(
        (message) ->
            message.isAppendMessage()
                && containsQuorumConfiguration(message.getAppendMessage().getEntriesList(), configuration),
        aPreElectionReply()::matches);
  }

  private static Set<Long> initialPeerSet() {
    return Sets.newHashSet(1L, 2L, 3L, 4L, 5L, 6L, 7L);
  }

  private static Set<Long> smallerPeerSetWithOneInCommonWithInitialSet() {
    return Sets.newHashSet(7L, 8L, 9L);
  }

  private static Set<Long> smallerPeerSetWithNoneInCommonWithInitialSet() {
    return Sets.newHashSet(8L, 9L, 10L);
  }

  private static Set<Long> largerPeerSetWithSomeInCommonWithInitialSet() {
    return Sets.newHashSet(4L, 5L, 6L, 7L, 8L, 9L, 10L);
  }
}
