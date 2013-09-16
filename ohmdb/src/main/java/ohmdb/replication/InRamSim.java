/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.replication;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import ohmdb.replication.rpc.RpcReply;
import ohmdb.replication.rpc.RpcRequest;
import ohmdb.replication.rpc.RpcWireReply;
import ohmdb.replication.rpc.RpcWireRequest;
import org.jetlang.channels.AsyncRequest;
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

/**

 */
public class InRamSim {
    private static final Logger LOG = LoggerFactory.getLogger(InRamSim.class);

    public static class Info implements RaftInformationInterface {

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
    }

    public static class Persister implements RaftInfoPersistence {
        @Override
        public long readCurrentTerm() {
            return 0;
        }

        @Override
        public long readVotedFor() {
            return 0;
        }
    }

    final int peerSize;
    final Map<Long, Replicator> replicators = new HashMap<>();
    final RequestChannel<RpcRequest, RpcWireReply> rpcChannel = new MemoryRequestChannel<>();
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
            Replicator rep = new Replicator(fiberPool.create(),
                    peerId,
                    "foobar",
                    peerIds,
                    new InRamLog(),
                    new Info(plusMillis),
                    new Persister(),
                    rpcChannel);
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

        rpcFiber.start();
    }
    private final Meter messages = metrics.meter(name(InRamSim.class, "messageRate"));
    private final Counter messageTxn = metrics.counter(name(InRamSim.class, "messageTxn"));

    private void messageForwarder(final Request<RpcRequest, RpcWireReply> origMsg) {

        // ok, who sent this?!!!!!
        final RpcRequest request = origMsg.getRequest();
//        final OutgoingRpcRequest request = origMsg.getRequest();
//        msgSize.update(request.message.getSerializedSize());
        final long dest = request.to;
        // find it:
        final Replicator repl = replicators.get(dest);
//        final FleaseLease fl = fleaseRunners.get(dest);
        if (repl == null) {
            // boo
            LOG.error("Request to non exist {}", dest);
            origMsg.reply(null);
            return;
        }

        messages.mark();
        messageTxn.inc();

        //LOG.debug("Forwarding message from {} to {}, contents: {}", request.from, request.to, request.message);
        // Construct and send a IncomingRpcRequest from the OutgoingRpcRequest.
        // There is absolutely no way to know who this is from at this point from the infrastructure.
        //final IncomingRpcRequest newRequest = new IncomingRpcRequest(1, request.from, request.message);
        final RpcWireRequest newRequest = new RpcWireRequest(request.to, request.from, 1, request.message);
        AsyncRequest.withOneReply(rpcFiber, repl.getIncomingChannel(), newRequest, new Callback<RpcReply>() {
            @Override
            public void onMessage(RpcReply msg) {
                // Translate the OutgoingRpcReply -> IncomingRpcReply.
                //LOG.debug("Forwarding reply message from {} back to {}, contents: {}", dest, request.to, msg.message);
                messages.mark();
                messageTxn.dec();
//                msgSize.update(msg.message.getSerializedSize());
                RpcWireReply newReply = new RpcWireReply(msg.to, msg.from, msg.messageId, msg.message);
//                IncomingRpcReply newReply = new IncomingRpcReply(msg.message, dest);
                origMsg.reply(newReply);
            }
        });
    }

    public void run() throws ExecutionException, InterruptedException {
        Replicator theOneIKilled = null;

        for(int i = 0 ; i < 15 ; i++) {
            Thread.sleep(3 * 1000);

            for (Replicator repl : replicators.values()) {
//                if (theOneIKilled != null && theOneIKilled.getId() == repl.getId()) {
//                    if (i > 10)
//                        System.out.print("BACK_TO_LIFE: ");
//                    else if (i > 5)
//                        System.out.print("DEAD: ");
//                }
                if (repl.isLeader()) System.out.print("LEADER: ");
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
        for(Replicator repl : replicators.values()) {
            repl.dispose();
        }
        fiberPool.dispose();


    }

}
