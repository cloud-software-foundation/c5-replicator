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
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang.time.StopWatch;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.BatchExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import static c5db.interfaces.ReplicationModule.ReplicatorInstanceEvent;

/**
 * A class to simulate a group of ReplicatorInstance nodes interacting via in-RAM channels, for testing purposes.
 */
public class InRamSim {
  private static final Logger LOG = LoggerFactory.getLogger(InRamSim.class);

  public static class Info implements ReplicatorInformationInterface {

    public final long offset;
    StopWatch stopWatch = new StopWatch();
    private boolean suspended = true; // field needed because StopWatch doesn't have a way to check its state

    public Info(long offset) {
      this.offset = offset;
      stopWatch.start();
      stopWatch.suspend();
    }

    public void startTimeout() {
      if (suspended) {
        suspended = false;
        stopWatch.resume();
      }
    }

    public void stopTimeout() {
      if (!suspended) {
        suspended = true;
        stopWatch.suspend();
      }
    }

    @Override
    public long currentTimeMillis() {
      return stopWatch.getTime() + offset;
    }

    @Override
    public long electionCheckRate() {
      return 100;
    }

    @Override
    public long electionTimeout() {
      return 1000;
    }

    @Override
    public long groupCommitDelay() {
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
    public SettableFuture<Boolean> future;
    public Predicate<RpcRequest> dropIf;
    public Predicate<RpcRequest> dropUntil;

    public WireObstruction(SettableFuture<Boolean> future, Predicate<RpcRequest> dropIf, Predicate<RpcRequest> dropUntil) {
      this.future = future;
      this.dropIf = dropIf;
      this.dropUntil = dropUntil;
    }
  }

  final int peerSize;
  private final Map<Long, ReplicatorInstance> replicators = new HashMap<>();
  private final Map<Long, ReplicatorLog> replicatorLogs = new HashMap<>();
  private final Map<Long, WireObstruction> wireObstructions = new HashMap<>();
  private final RequestChannel<RpcRequest, RpcWireReply> rpcChannel = new MemoryRequestChannel<>();
  final Channel<ReplicatorInstanceEvent> stateChanges = new MemoryChannel<>();
  final Channel<ReplicationModule.IndexCommitNotice> commitNotices = new MemoryChannel<>();
  final Fiber rpcFiber;
  final List<Long> peerIds = new ArrayList<>();
  private final PoolFiberFactory fiberPool;
  private final BatchExecutor batchExecutor;
  final Channel<RpcWireReply> replyChannel = new MemoryChannel<>();


  /**
   * Set up the simulation (but don't actually start it yet).
   *
   * @param peerSize              The number of nodes in the simulation
   * @param electionTimeoutOffset the time offset, in milliseconds, between different instances' clocks.
   * @param batchExecutor         The jetlang batch executor for the simulation's fibers to use.
   */
  public InRamSim(final int peerSize, long electionTimeoutOffset, BatchExecutor batchExecutor) {
    this.peerSize = peerSize;
    this.fiberPool = new PoolFiberFactory(Executors.newCachedThreadPool());
    this.batchExecutor = batchExecutor;
    Random r = new Random();

    for (int i = 0; i < peerSize; i++) {
      peerIds.add((long) r.nextInt());
    }

    long plusMillis = 0;
    for (long peerId : peerIds) {
      // make me a ....
      ReplicatorLog log = new InRamLog();
      ReplicatorInstance rep = new ReplicatorInstance(fiberPool.create(batchExecutor),
          peerId,
          "foobar",
          peerIds,
          log,
          new Info(plusMillis),
          new Persister(),
          rpcChannel,
          stateChanges,
          commitNotices);
      replicators.put(peerId, rep);
      replicatorLogs.put(peerId, log);
      plusMillis += electionTimeoutOffset;
    }

    rpcFiber = fiberPool.create(batchExecutor);

    // subscribe to the rpcChannel:
    rpcChannel.subscribe(rpcFiber, this::messageForwarder);
    commitNotices.subscribe(rpcFiber, message -> LOG.debug("Commit notice {}", message));
  }

  // Stop and dispose of the requested peer; but leave its object in the replicators map so that e.g.
  // its persistent storage can be read later.
  public void killPeer(long peerId) {
    assert replicators.containsKey(peerId);
    ReplicatorInstance repl = replicators.get(peerId);
    repl.dispose();
    replicatorLogs.remove(peerId);
  }

  public void restartPeer(long peerId) {
    assert replicators.containsKey(peerId);
    ReplicatorInstance oldRepl = replicators.get(peerId);
    ReplicatorLog log = new InRamLog();
    ReplicatorInstance repl = new ReplicatorInstance(fiberPool.create(batchExecutor),
        peerId,
        "foobar",
        peerIds,
        log,
        oldRepl.info,
        oldRepl.persister,
        rpcChannel,
        stateChanges,
        commitNotices);
    replicators.put(peerId, repl);
    replicatorLogs.put(peerId, log);
    repl.start();
  }

  // This method initiates a period of time during which requests attempting to reach peerId
  // will be selectively dropped. It returns a future to let the client of this method know when
  // the obstruction is removed. This method simply stores the information it's passed. The
  // shouldDropRequest method is responsible for applying it.
  public ListenableFuture<Boolean> dropIncomingRequests(long peerId,
                                                        Predicate<RpcRequest> dropIf,
                                                        Predicate<RpcRequest> dropUntil) {
    SettableFuture<Boolean> future = SettableFuture.create();
    if (wireObstructions.containsKey(peerId)) {
      wireObstructions.get(peerId).future.set(true);
    }
    wireObstructions.put(peerId, new WireObstruction(future, dropIf, dropUntil));
    return future;
  }

  private boolean shouldDropRequest(RpcRequest request) {
    long peerId = request.to;

    if (!wireObstructions.containsKey(peerId)) {
      return false;
    } else {
      WireObstruction drop = wireObstructions.get(peerId);
      if (drop.dropUntil.test(request)) {
        drop.future.set(true);
        wireObstructions.remove(peerId);
        return false;
      } else {
        return drop.dropIf.test(request);
      }
    }
  }

  public RequestChannel<RpcRequest, RpcWireReply> getRpcChannel() {
    return rpcChannel;
  }

  public Channel<RpcWireReply> getReplyChannel() {
    return replyChannel;
  }

  public Channel<ReplicationModule.IndexCommitNotice> getCommitNotices() {
    return commitNotices;
  }

  public Map<Long, ReplicatorInstance> getReplicators() {
    return replicators;
  }

  public ReplicatorLog getLog(long peerId) {
    assert replicatorLogs.containsKey(peerId);
    return replicatorLogs.get(peerId);
  }

  public void stopAllTimeouts() {
    for (ReplicatorInstance repl : replicators.values()) {
      ((Info) repl.info).stopTimeout();
    }
  }

  public void startAllTimeouts() {
    for (ReplicatorInstance repl : replicators.values()) {
      ((Info) repl.info).startTimeout();
    }
  }

  public void stopTimeout(long peerId) {
    assert replicators.containsKey(peerId);
    ((Info) replicators.get(peerId).info).stopTimeout();
  }

  public void startTimeout(long peerId) {
    ((Info) replicators.get(peerId).info).startTimeout();
  }

  private void messageForwarder(final Request<RpcRequest, RpcWireReply> origMsg) {

    final RpcRequest request = origMsg.getRequest();
    final long dest = request.to;
    final ReplicatorInstance repl = replicators.get(dest);
    if (repl == null) {
      // boo
      LOG.error("Request to non exist {}", dest);
      origMsg.reply(null);
      return;
    }

    LOG.info("Request {}", request);
    if (shouldDropRequest(request)) {
      LOG.warn("Incoming request dropped: Request {}", request);
      return;
    }

    final RpcWireRequest newRequest = new RpcWireRequest(request.from, request.quorumId, request.message);
    AsyncRequest.withOneReply(rpcFiber, repl.getIncomingChannel(), newRequest, msg -> {
      // Note that 'RpcReply' has an empty from/to/messageId.  We must know from our context (and so we do)
      RpcWireReply newReply = new RpcWireReply(request.to, request.quorumId, msg.message);
      LOG.info("Reply {}", newReply);
      replyChannel.publish(newReply);
      origMsg.reply(newReply);
    });
  }

  public void start() {
    for (ReplicatorInstance repl : replicators.values()) {
      repl.start();
    }
    rpcFiber.start();
  }

  public static void main(String[] args) throws InterruptedException {
    InRamSim sim = new InRamSim(3, 500, new BatchExecutorImpl());
    sim.start();
    Thread.sleep(10 * 1000);
    sim.dispose();
  }

  public void dispose() {
    rpcFiber.dispose();
    for (ReplicatorInstance repl : replicators.values()) {
      repl.dispose();
    }
    fiberPool.dispose();
  }
}
