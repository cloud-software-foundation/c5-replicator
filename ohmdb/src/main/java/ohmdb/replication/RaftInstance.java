package ohmdb.replication;

import com.google.common.collect.ImmutableList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Single instantation of a raft / log / lease
 */
public class RaftInstance {
    private static final Logger LOG = LoggerFactory.getLogger(RaftInstance.class);

    private final RequestChannel<RpcRequest,RpcWireReply> sendRpcChannel;
    private final RequestChannel<RpcWireRequest,RpcReply> incomingChannel = new MemoryRequestChannel<>();

    private final Fiber fiber;
    private final long myId;
    private final String quorumId;

    private final ImmutableList<Long> peers;
    private int majority;

    // What state is this instance in?
    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER,
    }

    // Initial state == CANDIDATE
    State myState = State.FOLLOWER;

    // In theory these are persistent:
    private long currentTerm;
    private long votedFor;
    private final RaftLogAbstraction log;


    final RaftInformationInterface info;

    final RaftInfoPersistence persister;

    private long lastRPC = 0;

    public RaftInstance(Fiber fiber,
                        long myId,
                        String quorumId,
                        List<Long> peers,
                        RaftLogAbstraction log,
                        RaftInformationInterface info,
                        RaftInfoPersistence persister,
                        RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel) {
        this.fiber = fiber;
        this.myId = myId;
        this.quorumId = quorumId;
        this.peers = ImmutableList.copyOf(peers);
        this.sendRpcChannel = sendRpcChannel;
        this.log = log;
        this.info = info;
        this.persister = persister;

        assert this.peers.contains(this.myId);

        incomingChannel.subscribe(fiber, new Callback<Request<RpcWireRequest, RpcReply>>() {
            @Override
            public void onMessage(Request<RpcWireRequest, RpcReply> message) {
                onIncomingMessage(message);
            }
        });

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                readPersistentData();
            }
        });

        fiber.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // check on election
                checkOnElection();
            }
        }, info.electionCheckRate(), info.electionCheckRate(), TimeUnit.MILLISECONDS);

        fiber.start();
    }

    @FiberOnly
    private void readPersistentData() {
        currentTerm = persister.readCurrentTerm();
        votedFor = persister.readVotedFor();
    }

    @FiberOnly
    private void onIncomingMessage(Request<RpcWireRequest, RpcReply> message) {
        RpcWireRequest req = message.getRequest();
        if (req.isRequestVoteMessage()) {
            lastRPC = info.currentTimeMillis();

            // do...
        } else if (req.isAppendMessage()) {
            lastRPC = info.currentTimeMillis();

            // do...
        } else {
            LOG.warn("{} Got a message of protobuf type I dont know: {}", myId, req);
        }

    }

    @FiberOnly
    private void checkOnElection() {

        if (lastRPC + info.electionTimeout() < info.currentTimeMillis()) {
            doElection();
        }
    }

    @FiberOnly
    private void doElection() {
        // increment current term:
        currentTerm = currentTerm + 1;
        myState = State.CANDIDATE;

        Raft.RequestVote msg = Raft.RequestVote.newBuilder()
        .setTerm(currentTerm)
                .setCandidateId(myId)
                .setLastLogIndex(log.getLastIndex())
                .setLastLogTerm(log.getLastTerm())
                .build();
        // issue RequestVoteRPCs in parallel:
        for (long peer : peers) {
            // create message:

            RpcRequest req = new RpcRequest(peer, myId, msg);
            AsyncRequest.withOneReply(fiber, sendRpcChannel, req, new Callback<RpcWireReply>() {
                @Override
                public void onMessage(RpcWireReply message) {

                    
                }
            });
        }

    }


}
