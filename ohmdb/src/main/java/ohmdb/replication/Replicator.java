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
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Single instantation of a raft / log / lease
 */
public class Replicator {
    private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);

    private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel;
    private final RequestChannel<RpcWireRequest, RpcReply> incomingChannel = new MemoryRequestChannel<>();

    private final Fiber fiber;
    private final long myId;
    private final String quorumId;

    private final ImmutableList<Long> peers;

    // What state is this instance in?
    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER,
    }

    // Initial state == CANDIDATE
    State myState = State.FOLLOWER;

    // In theory these are persistent:
    long currentTerm;
    long votedFor;

    // Election timers, etc.
    private long lastRPC = 0;
    private long myElectionTimeout;
    private Disposable electionChecker;


    private final RaftLogAbstraction log;
    final RaftInformationInterface info;
    final RaftInfoPersistence persister;


    public Replicator(Fiber fiber,
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
        Random r = new Random();
        this.myElectionTimeout = r.nextInt((int) info.electionTimeout()) + info.electionTimeout();

        assert this.peers.contains(this.myId);

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                readPersistentData();
            }
        });

        incomingChannel.subscribe(fiber, new Callback<Request<RpcWireRequest, RpcReply>>() {
            @Override
            public void onMessage(Request<RpcWireRequest, RpcReply> message) {
                onIncomingMessage(message);
            }
        });

//        fiber.execute(new Runnable() {
//            @Override
//            public void run() {
//                doElection();
//            }
//        });

        // start election checker:
//        electionChecker = fiber.schedule(new Runnable() {
//            @Override
//            public void run() {
//                checkOnElection();
//            }
//        }, myElectionTimeout, TimeUnit.MILLISECONDS);

        electionChecker = fiber.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
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
            doRequestVote(message);

        } else if (req.isAppendMessage()) {
            doAppendMessage(message);


        } else {
            LOG.warn("{} Got a message of protobuf type I dont know: {}", myId, req);
        }

    }

    @FiberOnly
    private void doRequestVote(Request<RpcWireRequest, RpcReply> message) {
        Raft.RequestVote msg = message.getRequest().getRequestVoteMessage();

        // 1. Return if term < currentTerm (sec 5.1)
        if (msg.getTerm() < currentTerm) {
            Raft.RequestVoteReply m = Raft.RequestVoteReply.newBuilder()
                    .setQuorumId(quorumId)
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
            RpcReply reply = new RpcReply(message.getRequest(), m);
            message.reply(reply);
            return;
        }

        // 2. if term > currentTerm, currentTerm <- term
        if (msg.getTerm() > currentTerm) {
            LOG.debug("{} requestVote rpc, pushing forward currentTerm {} to {}", currentTerm, msg.getTerm());
            setCurrentTerm(msg.getTerm());

            // 2a. Step down if candidate or leader.
            if (myState != State.FOLLOWER) {
                LOG.debug("{} stepping down to follower, currentTerm: {}", myId, currentTerm);
                myState = State.FOLLOWER;
            }
        }

        // 3. if votedFor is null (0), or candidateId, and candidate's log
        // is at least as complete as local log (sec 5.2, 5.4), grant vote
        // and reset election timeout.

        // TODO check if candidate's log is at least as complete as my log. (sec 5.2, 5.4)

        boolean vote = false;
        if (votedFor == 0 || votedFor == message.getRequest().from) {
            setVotedFor(message.getRequest().from);
            lastRPC = info.currentTimeMillis();
            vote = true;
        }

        LOG.debug("{} sending vote reply to to {}, voted = {}", myId, votedFor, vote);
        Raft.RequestVoteReply m = Raft.RequestVoteReply.newBuilder()
                .setQuorumId(quorumId)
                .setTerm(currentTerm)
                .setVoteGranted(vote)
                .build();
        RpcReply reply = new RpcReply(message.getRequest(), m);
        message.reply(reply);
    }

    @FiberOnly
    private void doAppendMessage(Request<RpcWireRequest, RpcReply> message) {
        Raft.AppendEntries msg = message.getRequest().getAppendMessage();

        // 1. return if term < currentTerm (sec 5.1)
        if (msg.getTerm() < currentTerm) {
            // TODO send error reply.
            return;
        }

        // 2. if term > currentTerm, set it (sec 5.1)
        if (msg.getTerm() > currentTerm) {
            setCurrentTerm(msg.getTerm());


        }
        // 3. Step down if we are a leader or a candidate (sec 5.2, 5.5)
        if (myState != State.FOLLOWER) {
            myState = State.FOLLOWER;
        }

        // 4. reset election timeout
        lastRPC = info.currentTimeMillis();

        if (msg.getEntriesCount() == 0) {
            // TODO determine if we are replying to this message.
            return; // do nothing?
        }

        // 5. return failure if log doesn't contain an entry at
        // prevLogIndex who's term matches prevLogTerm (sec 5.3)

        // 6. if existing entries conflict with new entries, delete all
        // existing entries starting with first conflicting entry (sec 5.3)

        // 7. append any new entries not already in the log


    }

    @FiberOnly
    private void checkOnElection() {
        if (lastRPC + info.electionTimeout() < info.currentTimeMillis()) {
            doElection();
        }
    }

    private int calculateMajority(int peerCount) {
        return (int) Math.ceil((peerCount + 1) / 2.0);
    }

    @FiberOnly
    private void doElection() {
        final int majority = calculateMajority(peers.size());
        // Start new election "timer".
        lastRPC = info.currentTimeMillis();
        // increment term.
        setCurrentTerm(currentTerm + 1);
        myState = State.CANDIDATE;

        Raft.RequestVote msg = Raft.RequestVote.newBuilder()
                .setTerm(currentTerm)
                .setCandidateId(myId)
                .setLastLogIndex(log.getLastIndex())
                .setLastLogTerm(log.getLastTerm())
                .build();

        LOG.debug("{} Starting election for currentTerm: {}", myId, currentTerm);

        final long termBeingVotedFor = currentTerm;
        final List<Long> votes = new ArrayList<>();
        for (long peer : peers) {
            // create message:

            RpcRequest req = new RpcRequest(peer, myId, msg);
            AsyncRequest.withOneReply(fiber, sendRpcChannel, req, new Callback<RpcWireReply>() {
                @Override
                public void onMessage(RpcWireReply message) {
                    // if current term has advanced, these replies are stale and should be ignored:

                    if (currentTerm > termBeingVotedFor) {
                        LOG.warn("{} election reply from {}, but currentTerm {} > vote term {}", myId, message.from,
                                termBeingVotedFor, currentTerm);
                        return;
                    }

                    // if we are no longer a Candidate, election was over, these replies are stale.
                   if (myState != State.CANDIDATE) {
                        // we became not, ignore
                        LOG.warn("{} election reply from {} ignored -> in state {}", myId, message.from, myState);
                        return;
                    }

                    Raft.RequestVoteReply reply = message.getRequestVoteReplyMessage();

                    if (reply.getTerm() > currentTerm) {
                        LOG.warn("{} election reply from {}, but term {} was not my term {}, updating currentTerm", myId,
                                message.from, reply.getTerm(), currentTerm);

                        setCurrentTerm(reply.getTerm());
                        return;
                    } else if (reply.getTerm() < currentTerm) {
                        // huh weird.
                        LOG.warn("{} election reply from {}, their term {} < currentTerm {}", myId, reply.getTerm(), currentTerm);
                    }

                    // did you vote for me?
                    if (reply.getVoteGranted()) {
                        // yes!
                        votes.add(message.from);
                    }

                    if (votes.size() > majority ) {
                        // i am now the leader
                        LOG.warn("{} I AM THE LEADER NOW!", myId);
                        // TODO do more stuff now.
                        myState = State.LEADER;
                    }
                }
            });
        }
    }

    // Small helper methods goeth here

    public RequestChannel<RpcWireRequest, RpcReply> getIncomingChannel() {
        return incomingChannel;
    }

    private void setVotedFor(long votedFor) {
        this.votedFor = votedFor;
    }

    private void setCurrentTerm(long newTerm) {
        this.currentTerm = newTerm;
        // new term, means new attempt to elect.
        setVotedFor(0);
    }

    public long getId() {
        return myId;
    }

    public void dispose() {
        fiber.dispose();
    }

    public boolean isLeader() {
        return myState == State.LEADER;
    }

}
