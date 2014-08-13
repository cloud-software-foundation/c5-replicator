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
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.log.InRamLog;
import c5db.replication.rpc.RpcMessage;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.time.StopWatch;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * A class to simulate a group of ReplicatorInstance nodes interacting via in-RAM channels, for testing purposes.
 */
public class InRamSim {
  private static final Logger LOG = LoggerFactory.getLogger("InRamSim");

  public static class StoppableClock implements ReplicatorClock {
    private final long electionTimeout;
    private final StopWatch stopWatch = new StopWatch();
    private boolean suspended = true; // field needed because StopWatch doesn't have a way to check its state
    private long lastTimeMillis = 0;
    private long offset;


    public StoppableClock(long offset, long electionTimeout) {
      Random r = new Random();
      this.offset = offset;
      this.electionTimeout = r.nextInt((int) electionTimeout) + electionTimeout;
      stopWatch.start();
      stopWatch.suspend();
    }

    public synchronized void startTimeout() {
      if (suspended) {
        suspended = false;
        stopWatch.resume();
      }
    }

    public synchronized void stopTimeout() {
      if (!suspended) {
        suspended = true;
        lastTimeMillis = stopWatch.getTime();
        stopWatch.suspend();
      }
    }

    public synchronized void advanceTime(long millis) {
      offset += millis;
    }

    @Override
    public synchronized long currentTimeMillis() {
      if (suspended) {
        return lastTimeMillis + offset;
      } else {
        return stopWatch.getTime() + offset;
      }
    }

    @Override
    public long electionCheckInterval() {
      return 100;
    }

    @Override
    public long electionTimeout() {
      return electionTimeout;
    }

    @Override
    public long leaderLogRequestsProcessingInterval() {
      return 50;
    }
  }

  public static class Persister implements ReplicatorInfoPersistence {
    private long votedFor = 0;
    private long currentTerm = 0;

    @Override
    public long readCurrentTerm(String quorumId) {
      return currentTerm;
    }

    @Override
    public long readVotedFor(String quorumId) {
      return votedFor;
    }

    @Override
    public void writeCurrentTermAndVotedFor(String quorumId, long currentTerm, long votedFor) {
      this.currentTerm = currentTerm;
      this.votedFor = votedFor;
    }
  }

  private class WireObstruction {
    public final Predicate<RpcMessage> dropIf;
    public final Predicate<RpcMessage> dropUntil;

    public WireObstruction(Predicate<RpcMessage> dropIf, Predicate<RpcMessage> dropUntil) {
      this.dropIf = dropIf;
      this.dropUntil = dropUntil;
    }
  }

  private final Set<Long> peerIds = new HashSet<>();
  private final Set<Long> offlinePeers = new HashSet<>();
  private final Map<Long, ReplicatorInstance> replicators = new HashMap<>();
  private final Map<Long, ReplicatorLog> replicatorLogs = new HashMap<>();
  private final List<WireObstruction> wireObstructions
      = Collections.<WireObstruction>synchronizedList(new ArrayList<>());
  private final RequestChannel<RpcRequest, RpcWireReply> rpcChannel = new MemoryRequestChannel<>();
  private final Channel<IndexCommitNotice> commitNotices = new MemoryChannel<>();
  private final Fiber rpcFiber;
  private final PoolFiberFactory fiberPool;
  private final BatchExecutor batchExecutor;
  private final Channel<RpcMessage> replyChannel = new MemoryChannel<>();
  private final Channel<ReplicatorInstanceEvent> eventChannel = new MemoryChannel<>();

  private final long electionTimeout;
  private final long electionTimeoutOffset;

  /**
   * Set up the simulation (but don't actually start it yet).
   *
   * @param electionTimeout       The timeout in milliseconds before an instance holds a new election
   * @param electionTimeoutOffset the time offset, in milliseconds, between different instances' clocks.
   * @param batchExecutor         The jetlang batch executor for the simulation's fibers to use.
   */
  public InRamSim(long electionTimeout, long electionTimeoutOffset, BatchExecutor batchExecutor) {
    this.fiberPool = new PoolFiberFactory(Executors.newCachedThreadPool());
    this.batchExecutor = batchExecutor;
    this.electionTimeout = electionTimeout;
    this.electionTimeoutOffset = electionTimeoutOffset;
    this.rpcFiber = fiberPool.create(batchExecutor);

    rpcChannel.subscribe(rpcFiber, this::messageForwarder);
    commitNotices.subscribe(rpcFiber, message -> LOG.debug("Commit notice {}", message));
  }

  public void createAndStartReplicators(Collection<Long> replPeerIds) {
    long plusMillis = 0;
    for (long peerId : replPeerIds) {
      if (replicators.containsKey(peerId)) {
        continue;
      }

      ReplicatorLog log = new InRamLog();
      ReplicatorInstance rep = new ReplicatorInstance(fiberPool.create(batchExecutor),
          peerId,
          "foobar",
          log,
          new StoppableClock(plusMillis, electionTimeout),
          new Persister(),
          rpcChannel,
          eventChannel,
          commitNotices,
          Replicator.State.FOLLOWER);
      peerIds.add(peerId);
      replicators.put(peerId, rep);
      replicatorLogs.put(peerId, log);
      plusMillis += electionTimeoutOffset;
      rep.start();
    }
  }

  // Stop and dispose of the requested peer; but leave its object in the replicators map so that e.g.
  // its persistent storage can be read later.
  public void killPeer(long peerId) {
    assert replicators.containsKey(peerId);
    ReplicatorInstance repl = replicators.get(peerId);
    repl.dispose();
    offlinePeers.add(peerId);
  }

  public void restartPeer(long peerId) {
    assert replicators.containsKey(peerId);
    ReplicatorInstance oldRepl = replicators.get(peerId);
    ReplicatorLog log = new InRamLog();
    ReplicatorInstance repl = new ReplicatorInstance(fiberPool.create(batchExecutor),
        peerId,
        "foobar",
        log,
        oldRepl.clock,
        oldRepl.persister,
        rpcChannel,
        eventChannel,
        commitNotices,
        Replicator.State.FOLLOWER);
    replicators.put(peerId, repl);
    replicatorLogs.put(peerId, log);
    offlinePeers.remove(peerId);
    repl.start();
  }

  public Set<Long> getOnlinePeers() {
    return Sets.difference(peerIds, offlinePeers);
  }

  public Set<Long> getOfflinePeers() {
    return offlinePeers;
  }

  // This method initiates a period of time during which messages attempting to reach peerId
  // will be selectively dropped. This method simply stores the information it's passed. The
  // shouldDropMessage method is responsible for applying it.
  public void dropMessages(Predicate<RpcMessage> dropIf,
                           Predicate<RpcMessage> dropUntil) {

    wireObstructions.add(new WireObstruction(dropIf, dropUntil));
  }

  private boolean shouldDropMessage(RpcMessage message) {
    boolean dropMessage = false;

    synchronized (wireObstructions) {
      Iterator<WireObstruction> iterator = wireObstructions.listIterator();

      while (iterator.hasNext()) {
        WireObstruction obstruction = iterator.next();
        if (obstruction.dropUntil.test(message)) {
          iterator.remove();
        } else if (obstruction.dropIf.test(message)) {
          dropMessage = true;
        }
      }
    }

    return dropMessage;
  }

  public RequestChannel<RpcRequest, RpcWireReply> getRpcChannel() {
    return rpcChannel;
  }

  public Channel<RpcMessage> getReplyChannel() {
    return replyChannel;
  }

  public Channel<ReplicatorInstanceEvent> getEventChannel() {
    return eventChannel;
  }

  public Channel<IndexCommitNotice> getCommitNotices() {
    return commitNotices;
  }

  public Map<Long, ReplicatorInstance> getReplicators() {
    return replicators;
  }

  public ReplicatorLog getLog(long peerId) {
    assertThat(replicatorLogs.keySet(), hasItem(peerId));
    return replicatorLogs.get(peerId);
  }

  public void stopAllTimeouts() {
    for (ReplicatorInstance repl : replicators.values()) {
      ((StoppableClock) repl.clock).stopTimeout();
    }
  }

  public void startAllTimeouts() {
    for (ReplicatorInstance repl : replicators.values()) {
      ((StoppableClock) repl.clock).startTimeout();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public void stopTimeout(long peerId) {
    assert replicators.containsKey(peerId);
    ((StoppableClock) replicators.get(peerId).clock).stopTimeout();
  }

  public void startTimeout(long peerId) {
    ((StoppableClock) replicators.get(peerId).clock).startTimeout();
  }

  private void messageForwarder(final Request<RpcRequest, RpcWireReply> origMsg) {

    final RpcRequest request = origMsg.getRequest();
    final long destination = request.to;
    final ReplicatorInstance repl = replicators.get(destination);
    if (repl == null) {
      // boo
      LOG.warn("Request to nonexistent peer {}", destination);
      // Do nothing and allow request to timeout.
      return;
    }

    LOG.info("Request {}", request);
    if (shouldDropMessage(request)) {
      LOG.warn("Request dropped: {}", request);
      return;
    }

    final RpcWireRequest newRequest = new RpcWireRequest(request.from, request.quorumId, request.message);
    AsyncRequest.withOneReply(rpcFiber, repl.getIncomingChannel(), newRequest, msg -> {
      // Note that 'RpcReply' has an empty from/to/messageId.  We must know from our context (and so we do)
      RpcWireReply newReply = new RpcWireReply(request.from, request.to, request.quorumId, msg.message);
      LOG.info("Reply {}", newReply);
      if (shouldDropMessage(newReply)) {
        LOG.warn("Reply dropped: {}", newReply);
        return;
      }
      replyChannel.publish(newReply);
      origMsg.reply(newReply);
    });
  }

  public void start(Collection<Long> initialPeers) throws ExecutionException, InterruptedException, TimeoutException {
    createAndStartReplicators(initialPeers);

    rpcFiber.start();

    // bootstrap ALL the replicators, collect their futures and skip the null ones.
    List<ListenableFuture<Void>> futures = replicators.values()
        .stream().map(repl -> repl.bootstrapQuorum(peerIds))
        .filter(future -> future != null)
        .collect(Collectors.toList());

    for (ListenableFuture<Void> aFuture : futures) {
      LOG.info("Waiting for bootstrap");
      aFuture.get(4, TimeUnit.SECONDS);
    }

//    pickAReplicator().bootstrapQuorum(peerIds).get(4, TimeUnit.SECONDS);
  }

  public void dispose() {
    rpcFiber.dispose();
    for (ReplicatorInstance repl : replicators.values()) {
      repl.dispose();
    }
    fiberPool.dispose();
  }
}
