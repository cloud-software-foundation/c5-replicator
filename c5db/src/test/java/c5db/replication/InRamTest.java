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
import c5db.log.ReplicatorLog;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.RequestVote;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.ThrowFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.CharsetUtil;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * A class for tests of the behavior of multiple interacting ReplicatorInstance nodes,
 */
public class InRamTest {
  private static final int ELECTION_LIMIT = 20;  // maximum number of election attempts
  private static final int TEST_TIMEOUT = 10; // maximum timeout allowed for any election (seconds)
  private static final int NUM_PEERS = 7; // number of nodes in simulations
  private static final long OFFSET_STAGGERING_MILLIS = 250; // offset between different peers' clocks
  private static final List<ByteBuffer> TEST_DATUM = Lists.newArrayList(
      ByteBuffer.wrap("test".getBytes(CharsetUtil.UTF_8)));

  @Rule
  public ThrowFiberExceptions fiberExceptionHandler = new ThrowFiberExceptions();

  private BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);
  private RunnableExecutor runnableExecutor = new RunnableExecutorImpl(batchExecutor);
  private final Fiber fiber = new ThreadFiber(runnableExecutor, null, true);
  private InRamSim sim;
  private final Channel<Long> simStatusChanges = new MemoryChannel<>();

  private long currentTerm = 0;
  private long currentLeader = 0;
  private final Map<Long, Long> lastCommit = new HashMap<>();


  // Observe and record log commits made by peers.
  private void monitorCommits(ReplicationModule.IndexCommitNotice commitNotice) {
    long peerId = commitNotice.replicatorInstance.getId();
    long index = commitNotice.committedIndex;
    long prevCommitIndex = lastCommit.getOrDefault(peerId, 0L);

    assert index >= prevCommitIndex;
    lastCommit.put(peerId, index);
    simStatusChanges.publish(0L);
  }

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

  // Return any peer in the FOLLOWER state, or 0 if there aren't any.
  private long pickFollower() {
    for (ReplicatorInstance repl : sim.getReplicators().values()) {
      if (repl.myState == ReplicatorInstance.State.FOLLOWER) {
        return repl.getId();
      }
    }
    return 0;
  }

  // Return a future to signal when a leader has been chosen. The caller must specify the minimum term number
  // in which the election "counts"; otherwise it is not well-determined when a leader is chosen. The future
  // will be set to true if a leader is successfully chosen before 'ELECTION_LIMIT' terms elapse; otherwise
  // the future will be set to false.
  private ListenableFuture<Boolean> getLeaderChosenFuture(long minimumTerm) throws InterruptedException {
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
  private ListenableFuture<Boolean> getAppendReplyFuture(Predicate<RpcWireReply> isValid)
      throws InterruptedException {
    SettableFuture<Boolean> future = SettableFuture.create();
    sim.getReplyChannel().subscribe(fiber, (reply) -> {
      if (reply.getAppendReplyMessage() != null && isValid.test(reply)) {
        future.set(true);
      }
    });
    return future;
  }

  // Counts leaders in the current term. Used to verify a sane state.
  // If the simulation is running correctly, this should only ever return 0 or 1.
  private long leaderCount() {
    long leaderCount = 0;
    for (ReplicatorInstance repl : sim.getReplicators().values()) {
      if (repl.isLeader() && repl.currentTerm >= this.currentTerm) {
        leaderCount++;
      }
    }
    assert leaderCount <= 1;
    return leaderCount;
  }

  // Blocks until the peer at peerId sends an AppendEntriesReply.
  // Throws TimeoutException if no reply occurs within the time limit.
  private void waitForAppendReply(long peerId) throws Exception {
    getAppendReplyFuture((reply) -> reply.from == peerId).get(TEST_TIMEOUT, TimeUnit.SECONDS);
  }

  // Blocks until a leader is elected during some term >= minimumTerm.
  // Throws TimeoutException if that does not occur within the time limit.
  private void waitForNewLeader(long minimumTerm) throws Exception {
    sim.startAllTimeouts();
    getLeaderChosenFuture(minimumTerm).get(TEST_TIMEOUT, TimeUnit.SECONDS);
    sim.stopAllTimeouts();

    assert currentLeader != 0;
    assert sim.getReplicators().get(currentLeader).isLeader();
    assert sim.getReplicators().get(currentLeader).currentTerm >= minimumTerm;
    assert leaderCount() == 1;

    sim.startTimeout(currentLeader);
  }

  // Blocks until the peer at peeId commits up to the given log index, or beyond.
  // Throws TimeoutException if that does not occur within the time limit.
  private void waitForCommit(long peerId, long index) throws Exception {
    SettableFuture<Boolean> future = SettableFuture.create();
    if (lastCommit.getOrDefault(peerId, 0L) >= index) {
      return;
    }
    simStatusChanges.subscribe(fiber, (term) -> {
      if (lastCommit.getOrDefault(peerId, 0L) >= index) {
        future.set(true);
      }
    });
    future.get(TEST_TIMEOUT, TimeUnit.SECONDS);
  }

  // Request that the leader log datum, then block until the leader notifies that it has processed the request.
  // Throws TimeoutException if the request isn't processed within the time limit.
  private void logDataAndWait(List<ByteBuffer> data) throws Exception {
    assert currentLeader != 0;
    sim.getReplicators().get(currentLeader).logData(data).get(TEST_TIMEOUT, TimeUnit.SECONDS);
  }

  @Before
  public final void setUp() {
    sim = new InRamSim(NUM_PEERS, OFFSET_STAGGERING_MILLIS, batchExecutor);
    sim.getRpcChannel().subscribe(fiber, this::monitorOutboundRequests);
    sim.getCommitNotices().subscribe(fiber, this::monitorCommits);
    sim.start();
    fiber.start();
  }

  @After
  public final void tearDown() {
    sim.dispose();
    fiber.dispose();
  }

  @Test
  public void testFirstLeaderElection() throws Exception {
    waitForNewLeader(1);
  }

  @Test
  public void testLeaderFailure() throws Exception {
    waitForNewLeader(1);
    sim.killPeer(currentLeader);
    waitForNewLeader(currentTerm + 1);
  }

  @Test
  public void testLeaderFailureAndRestart() throws Exception {
    waitForNewLeader(1);
    long theFirstLeader = currentLeader;
    sim.killPeer(theFirstLeader);
    waitForNewLeader(currentTerm + 1);
    assertNotEquals(currentLeader, theFirstLeader);
    sim.restartPeer(theFirstLeader);
    // Test fails if the following throws a TimeoutException
    waitForAppendReply(theFirstLeader);
  }

  @Test
  public void testFollowerFailure() throws Exception {
    waitForNewLeader(1);
    long theFirstLeader = currentLeader;
    long theFirstTerm = currentTerm;
    long followerId = pickFollower();
    sim.killPeer(followerId);

    // Have the leader log an entry, and all peers except the (currently offline) follower will ack and commit it.
    logDataAndWait(TEST_DATUM);
    waitForCommit(theFirstLeader, 1);

    // Now while the one follower is still offline, the current leader also dies, and all other peers stage an election
    sim.killPeer(theFirstLeader);
    waitForNewLeader(theFirstTerm + 1);

    long theSecondLeader = currentLeader;
    assertNotEquals(theFirstLeader, theSecondLeader);
    assertNotEquals(followerId, theSecondLeader);

    // The second leader logs some entry, then the first node to go offline comes back
    logDataAndWait(TEST_DATUM);
    sim.restartPeer(followerId);
    waitForCommit(followerId, 2);

    logDataAndWait(TEST_DATUM);
    sim.restartPeer(theFirstLeader);
    waitForCommit(theFirstLeader, 3);

    // Verify now that every peer has logged identical data.
    for (ReplicatorInstance repl : sim.getReplicators().values()) {
      long peerId = repl.getId();
      assertEquals(3, (long) lastCommit.getOrDefault(peerId, 0L));
      assertEquals(3, sim.getLog(peerId).getLastIndex());
    }

    // Verify that the the second leader is still leader
    assertEquals(theSecondLeader, currentLeader);
    assertEquals(1, leaderCount());
  }

  @Test
  public void testCommitIndexOnBecomingLeader() throws Exception {
    // Check case where a node with a certain commit index becomes leader, and maintains that commit index
    // Have the first leader log data, and have every other follower verify it has committed
    waitForNewLeader(1);
    long firstTerm = currentTerm;
    long firstLeader = currentLeader;
    logDataAndWait(TEST_DATUM);
    for (long peerId : sim.getReplicators().keySet()) {
      waitForCommit(peerId, 1);
    }

    // Kill the first leader; wait for a second leader to come to power
    sim.killPeer(currentLeader);
    waitForNewLeader(firstTerm + 1);
    long secondLeader = currentLeader;
    assert secondLeader != firstLeader;

    // Now listen for requests sent from the second leader. Report back the commit index it is sending, after
    // asserting that it is, in fact, sending AppendEntries messages.
    SettableFuture<Long> commitIndexFuture = SettableFuture.create();
    sim.getRpcChannel().subscribe(fiber, (request) -> {
      RpcRequest msg = request.getRequest();
      if (msg.from == secondLeader) {
        AppendEntries appendMessage = msg.getAppendMessage();
        if (appendMessage == null) {
          commitIndexFuture.setException(new AssertionError());
        }
        commitIndexFuture.set(appendMessage.getCommitIndex());
      }
    });
    assertEquals(1, (long) commitIndexFuture.get(TEST_TIMEOUT, TimeUnit.SECONDS));
  }

  @Test
  public void testSimpleReplication() throws Exception {
    // Replication "happy path": a leader broadcasts data successfully to all other nodes.

    waitForNewLeader(1);
    logDataAndWait(TEST_DATUM);

    for (long peerId : sim.peerIds) {
      if (peerId != currentLeader) {
        // Observe the commit notice channel to ensure the peer has logged and committed the data sent to it.
        //
        waitForCommit(peerId, 1);
      }
      ReplicatorLog log = sim.getLog(peerId);
      assertEquals(1, log.getLastIndex());
    }
  }

  @Test
  public void testSimpleReplicatorOutage() throws Exception {
    // A replicator has incoming messages dropped until it believes it needs to stage a new election.

    waitForNewLeader(1);
    long followerId = pickFollower();
    long termOfFirstLeader = currentTerm;
    assert followerId != 0;

    // Future used to wait until follower times out. When it times out it will send out a RequestVote with
    // currentTerm == 2, at which point this.currentTerm will increment.
    sim.dropIncomingRequests(followerId,
        (request) -> true,
        (request) -> false);
    sim.startTimeout(followerId);

    waitForNewLeader(termOfFirstLeader + 1);
    assertNotEquals(0, currentLeader);

    // Check that the follower that timed out is operating correctly:
    if (followerId != currentLeader) {
      waitForAppendReply(followerId);
    }

    assertTrue(followerId == currentLeader ||
        sim.getReplicators().get(followerId).myState == ReplicatorInstance.State.FOLLOWER);
  }

  @Test
  public void testFollowerCatchup() throws Exception {
    // A follower falls out of sync with logged and committed entries, then is elected leader.

    waitForNewLeader(1);
    long followerId = pickFollower();
    assert followerId != 0;

    // Drop any message with at least one entry, until the leader reports it's committed 2:
    ListenableFuture<Boolean> future = sim.dropIncomingRequests(followerId,
        (request) -> request.getAppendMessage().getEntriesList().size() > 0,
        (request) -> request.getAppendMessage().getCommitIndex() >= 2);

    logDataAndWait(TEST_DATUM);
    logDataAndWait(TEST_DATUM);
    waitForCommit(currentLeader, 2);
    future.get(TEST_TIMEOUT, TimeUnit.SECONDS);
    waitForCommit(followerId, 2);

    ReplicatorLog log = sim.getLog(followerId);
    assertEquals(2, log.getLastIndex());
    assertEquals(TEST_DATUM, log.getLogEntry(1).get().getDataList());
    assertEquals(TEST_DATUM, log.getLogEntry(2).get().getDataList());
  }
}
