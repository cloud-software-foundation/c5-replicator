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
import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class InRamSim {
  private static final Logger LOG = LoggerFactory.getLogger(InRamSim.class);

  public static class Info implements ReplicatorInformationInterface {

    public final long offset;

    public Info(long offset) {
      this.offset = offset;
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis() + offset;
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

  final int peerSize;
  final Map<Long, ReplicatorInstance> replicators = new HashMap<>();
  final RequestChannel<RpcRequest, RpcWireReply> rpcChannel = new MemoryRequestChannel<>();
  final Channel<ReplicationModule.ReplicatorInstanceEvent> stateChanges = new MemoryChannel<>();
  final Channel<ReplicationModule.IndexCommitNotice> commitNotices = new MemoryChannel<>();
  final Fiber rpcFiber;
  final List<Long> peerIds = new ArrayList<>();
  private final PoolFiberFactory fiberPool;
  private final MetricRegistry metrics = new MetricRegistry();


  public InRamSim(final int peerSize) {
    this.peerSize = peerSize;
    this.fiberPool = new PoolFiberFactory(Executors.newCachedThreadPool());
    Random r = new Random();

    for (int i = 0; i < peerSize; i++) {
      peerIds.add((long)r.nextInt());
    }

    long plusMillis = 0;
    for( long peerId : peerIds) {
      // make me a ....
      ReplicatorInstance rep = new ReplicatorInstance(fiberPool.create(),
          peerId,
          "foobar",
          peerIds,
          new InRamLog(),
          new Info(plusMillis),
          new Persister(),
          rpcChannel,
          stateChanges,
          commitNotices);
      rep.start();
      replicators.put(peerId, rep);
      plusMillis += 500;
    }

    rpcFiber = fiberPool.create();


    // subscribe to the rpcChannel:
    rpcChannel.subscribe(rpcFiber, new Callback<Request<RpcRequest, RpcWireReply>>() {
      @Override
      public void onMessage(Request<RpcRequest, RpcWireReply> message) {
        messageForwarder(message);
      }
    });
    commitNotices.subscribe(rpcFiber, new Callback<ReplicationModule.IndexCommitNotice>() {
      @Override
      public void onMessage(ReplicationModule.IndexCommitNotice message) {
        LOG.debug("Commit notice {}", message);
      }
    });


    rpcFiber.start();
  }

  public RequestChannel<RpcRequest, RpcWireReply> getRpcChannel() {
    return rpcChannel;
  }

  private final Meter messages = metrics.meter(name(InRamSim.class, "messageRate"));
  private final Counter messageTxn = metrics.counter(name(InRamSim.class, "messageTxn"));

  private void messageForwarder(final Request<RpcRequest, RpcWireReply> origMsg) {

    final RpcRequest request = origMsg.getRequest();
//        final OutgoingRpcRequest request = origMsg.getRequest();
//        msgSize.update(request.message.getSerializedSize());
    final long dest = request.to;
    final ReplicatorInstance repl = replicators.get(dest);
    if (repl == null) {
      // boo
      LOG.error("Request to non exist {}", dest);
      origMsg.reply(null);
      return;
    }

    messages.mark();
    messageTxn.inc();

    final RpcWireRequest newRequest = new RpcWireRequest(request.from, request.quorumId, request.message);
    AsyncRequest.withOneReply(rpcFiber, repl.getIncomingChannel(), newRequest, new Callback<RpcReply>() {
      @Override
      public void onMessage(RpcReply msg) {
        messages.mark();
        messageTxn.dec();
//                msgSize.update(msg.message.getSerializedSize());
        // Note that 'RpcReply' has an empty from/to/messageId.  We must know from our context (and so we do)
        RpcWireReply newReply = new RpcWireReply(request.to, request.quorumId, msg.message);
        origMsg.reply(newReply);
      }
    });
  }

  public void run() throws ExecutionException, InterruptedException {
    ReplicatorInstance theOneIKilled = null;

    for(int i = 0 ; i < 15 ; i++) {

      Thread.sleep(3 * 1000);

      for (ReplicatorInstance repl : replicators.values()) {
//                if (theOneIKilled != null && theOneIKilled.getId() == repl.getId()) {
//                    if (i > 10)
//                        System.out.print("BACK_TO_LIFE: ");
//                    else if (i > 5)
//                        System.out.print("DEAD: ");
//                }
        if (repl.isLeader()) {
          System.out.print("LEADER: ");
          repl.logData(new byte[]{1,2,3,4,5,6});
        }
        System.out.print(repl.getId() + " currentTerm: " + repl.currentTerm);
        System.out.println(" votedFor: " + repl.votedFor);
      }

      // TODO kill leader after we have keep-alive.
//            if (i == 5) {
//                for (Replicator repl : replicators.values()) {
//                    if (repl.isLeader()) {
//                        repl.dispose();
//                        theOneIKilled = repl;
//                    }
//
//                }
//            }
      // TODO crash recover after persister does persistence.
//            if (i == 10) {
//                // crash recovery with same UUID:
//                Replicator repl = new Replicator(fiberPool.create(),
//                        theOneIKilled.getId(),
//                        "foobar",
//                        peerIds,
//                        new Log(),
//                        theOneIKilled.info,
//                        new Persister(),
//                        rpcChannel);
//                replicators.put(theOneIKilled.getId(), repl);
//            }
    }
  }

  public static void main(String []args) throws ExecutionException, InterruptedException {
    InRamSim sim = new InRamSim(3);
    ConsoleReporter reporter = ConsoleReporter.forRegistry(sim.metrics)
        .convertDurationsTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();

    reporter.start(5, TimeUnit.SECONDS);
    sim.run();
    System.out.println("All done baby");
    sim.dispose();
    reporter.report();
    reporter.stop();
  }

  private void dispose() {
    rpcFiber.dispose();
    for(ReplicatorInstance repl : replicators.values()) {
      repl.dispose();
    }
    fiberPool.dispose();


  }

}
