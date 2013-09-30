package ohmdb.replication;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
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
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ohmdb.replication.Raft.LogEntry;

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
    // this is the next index from our log we need to send to each peer, kept track of on a per-peer basis.
    private HashMap<Long, Long> peersNextIndex;



    private static class IntLogRequest {
        public final byte[] datum;
        public final SettableFuture<Long> logNumberNotifation;
        public final SettableFuture<Boolean> durableNofication;

        private IntLogRequest(byte[] datum) {
            this.datum = datum;
            this.logNumberNotifation = SettableFuture.create();
            this.durableNofication = SettableFuture.create();
        }
    }
    private final BlockingQueue<IntLogRequest> logRequests = new ArrayBlockingQueue<>(100);


    // What state is this instance in?
    public enum State {
        INIT,
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


    private long lastCommittedIndex = 0;

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

        electionChecker = fiber.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                checkOnElection();
            }
        }, info.electionCheckRate(), info.electionCheckRate(), TimeUnit.MILLISECONDS);

        fiber.start();
    }

    // public API:

    /**
     * TODO change the type of datum to a protobuf that is useful.
     *
     * Log a datum
     * @param datum some data to log.
     * @return a listenable for the index number OR null if we aren't the leader.
     */
    public ListenableFuture<Long> logData(final byte[] datum) throws InterruptedException {
        if (!isLeader()) {
            return null;
        }

        IntLogRequest req = new IntLogRequest(datum);
        logRequests.put(req);

        // TODO return the durable notification future?
        return req.logNumberNotifation;
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

                haltLeader();
            }
        }

        // 3. if votedFor is null (0), or candidateId, and candidate's log
        // is at least as complete as local log (sec 5.2, 5.4), grant vote
        // and reset election timeout.

        boolean vote = false;
        if ( (log.getLastTerm() <= msg.getLastLogTerm())
                &&
                log.getLastIndex() <= msg.getLastLogIndex()) {
            // we can vote for this because the candidate's log is at least as
            // complete as the local log.

            if (votedFor == 0 || votedFor == message.getRequest().from) {
                setVotedFor(message.getRequest().from);
                lastRPC = info.currentTimeMillis();
                vote = true;
            }
        }

        LOG.debug("{} sending vote reply to {} vote = {}, voted = {}", myId, message.getRequest().from, votedFor, vote);
        Raft.RequestVoteReply m = Raft.RequestVoteReply.newBuilder()
                .setQuorumId(quorumId)
                .setTerm(currentTerm)
                .setVoteGranted(vote)
                .build();
        RpcReply reply = new RpcReply(message.getRequest(), m);
        message.reply(reply);
    }

    @FiberOnly
    private void doAppendMessage(final Request<RpcWireRequest, RpcReply> message) {
        Raft.AppendEntries msg = message.getRequest().getAppendMessage();

        // 1. return if term < currentTerm (sec 5.1)
        if (msg.getTerm() < currentTerm) {
            // TODO is this the correct message reply?
            Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                    .setQuorumId(quorumId)
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();

            RpcReply reply = new RpcReply(message.getRequest(), m);
            message.reply(reply);
            return;
        }

        // 2. if term > currentTerm, set it (sec 5.1)
        if (msg.getTerm() > currentTerm) {
            setCurrentTerm(msg.getTerm());
        }

        // 3. Step down if we are a leader or a candidate (sec 5.2, 5.5)
        if (myState != State.FOLLOWER) {
            haltLeader();
        }

        // 4. reset election timeout
        lastRPC = info.currentTimeMillis();

        if (msg.getEntriesCount() == 0) {
            Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                    .setQuorumId(quorumId)
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .build();

            RpcReply reply = new RpcReply(message.getRequest(), m);
            message.reply(reply);
            return;
        }

        // 5. return failure if log doesn't contain an entry at
        // prevLogIndex who's term matches prevLogTerm (sec 5.3)
        long msgPrevLogIndex = msg.getPrevLogIndex();
        long msgPrevLogTerm = msg.getPrevLogTerm();
        if (log.getLogTerm(msgPrevLogIndex) != msgPrevLogTerm) {
            Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                    .setQuorumId(quorumId)
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();

            RpcReply reply = new RpcReply(message.getRequest(), m);
            message.reply(reply);
            return;
        }

        // 6. if existing entries conflict with new entries, delete all
        // existing entries starting with first conflicting entry (sec 5.3)



        long nextIndex = log.getLastIndex()+1;

        List<LogEntry> entries =  msg.getEntriesList();
        ArrayList<LogEntry> entriesToCommit = new ArrayList<>(entries.size());
        for (LogEntry entry : entries) {
            long entryIndex = entry.getIndex();

            if (entryIndex == nextIndex) {
                LOG.debug("{} new log entry for idx {} term {}", myId, entryIndex, entry.getTerm());

                entriesToCommit.add(entry);

                nextIndex++;
                continue;
            }

            if (entryIndex > nextIndex) {
                // ok this entry is still beyond the LAST entry, so we have a problem:
                LOG.error("{} log entry missing, i expected {} and the next in the message is {}",
                        myId, nextIndex, entryIndex);

                // TODO handle this situation a little better if possible
                // reply with an error message leaving the log borked.
                Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                        .setQuorumId(quorumId)
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();

                RpcReply reply = new RpcReply(message.getRequest(), m);
                message.reply(reply);
                return;
            }

            // at this point entryIndex should be <= log.getLastIndex
            assert entryIndex < nextIndex;

            if (log.getLogTerm(entryIndex) != entry.getTerm()) {
                // conflict:
                LOG.debug("{} log conflict at idx {} my term: {} term from leader: {}, truncating log after this point", myId,
                        entryIndex, log.getLogTerm(entryIndex), entry.getTerm());

                // delete this and all subsequent entries:
                ListenableFuture<Boolean> truncateResult = log.truncateLog(entryIndex);
                // TODO don't wait for the truncate inline, wait for a callback (then what?)
                try {
                    boolean wasTruncated = truncateResult.get();
                    if (!wasTruncated) {
                        LOG.error("{} unable to truncate log to {}", myId, entryIndex);
                        // TODO figure out how to deal with this.
                    }
                } catch (InterruptedException|ExecutionException e) {
                    LOG.error("oops", e);
                }

                // add this entry to the list of things to commit to.
                entriesToCommit.add(entry);
                nextIndex = entryIndex+1; // continue normal processing after this baby.
            } //else {
                // this log entry did NOT conflict we dont need to re-commit this entry.
            //}
        }

        // 7. Append any new entries not already in the log.
        ListenableFuture<Boolean> logCommitNotification = log.logEntries(entriesToCommit);


        // 8. apply newly committed entries to state machine

        // TODO make/broadcast 'lastCommittedIdx' known throughout the local system.
        long lastCommittedIdx = msg.getCommitIndex();

        // wait for the log to commit before returning message.  But do so async.
        Futures.addCallback(logCommitNotification, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                        .setQuorumId(quorumId)
                        .setTerm(currentTerm)
                        .setSuccess(true)
                        .build();

                RpcReply reply = new RpcReply(message.getRequest(), m);
                message.reply(reply);
            }

            @Override
            public void onFailure(Throwable t) {
                // TODO better error reporting. A log commit failure will e a serious issue.
                Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                        .setQuorumId(quorumId)
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();

                RpcReply reply = new RpcReply(message.getRequest(), m);
                message.reply(reply);
            }
        });
    }

    @FiberOnly
    private void checkOnElection() {
        if (myState == State.LEADER) {
            LOG.debug("{} leader during election check.", myId);
            return;
        }

        if (lastRPC + info.electionTimeout() < info.currentTimeMillis()) {
            LOG.debug("{} Timed out checkin on election, try new election", myId);
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
                @FiberOnly
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

                        becomeLeader();
                    }
                }
            });
        }
    }


    //// Leader timer stuff below
    private Disposable queueConsumer;
    private Disposable appender = null;

    @FiberOnly
    private void haltLeader() {
        myState = State.FOLLOWER;

        stopAppendTimer();
        stopQueueConsumer();
    }

    @FiberOnly
    private void stopQueueConsumer() {
        if (queueConsumer != null) {
            queueConsumer.dispose();
            queueConsumer = null;
        }
    }

    private void becomeLeader() {
        LOG.warn("{} I AM THE LEADER NOW, commece AppendEntries RPCz", myId);

        myState = State.LEADER;

        // Page 7, para 5
        long myNextLog = log.getLastIndex()+1;

        peersNextIndex = new HashMap<>(peers.size());
        for (long peer : peers) {
            if (peer == myId) continue;

            peersNextIndex.put(peer, myNextLog);
        }

        //startAppendTimer();
        startQueueConsumer();
    }

    @FiberOnly
    private void startQueueConsumer() {
        queueConsumer = fiber.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                consumeQueue();
            }
        }, 0, info.groupCommitDelay(), TimeUnit.MILLISECONDS);
    }

    @FiberOnly
    private void consumeQueue() {
        // retrieve as many items as possible. send rpc.
        final ArrayList<IntLogRequest> reqs = new ArrayList<>();

        LOG.debug("{} queue consuming", myId);
        while (logRequests.peek() != null) {
            reqs.add(logRequests.poll());
        }

        LOG.debug("{} {} queue items to commit", myId, reqs.size());

        final long firstInList = log.getLastIndex()+1;
        long idAssigner = firstInList;

        // Build the log entries:
        ArrayList <LogEntry> newLogEntries = new ArrayList<>(reqs.size());
        for (IntLogRequest logReq : reqs) {
            LogEntry entry = LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setIndex(idAssigner)
                    .setData(ByteString.copyFrom(logReq.datum))
                    .build();
            newLogEntries.add(entry);

            // let the client know what our id is
            logReq.logNumberNotifation.set(idAssigner);

            idAssigner ++;
        }

        // Should throw immediately if there was a basic validation error.
        final ListenableFuture<Boolean> localLogFuture = log.logEntries(newLogEntries);
        final long largestIndexInBatch = idAssigner - 1;

        Raft.AppendEntries.Builder msgProto = Raft.AppendEntries.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(myId)
                .setPrevLogIndex(log.getLastIndex())
                .setPrevLogTerm(log.getLastTerm())
                .setCommitIndex(0);

        final long lastIndexSent = log.getLastIndex();

        // What a majority means at this moment.
        final long majority = calculateMajority(peers.size());
        // Am I attempting to commit new entries from my term? This is only yes if I have 'newLogEntries' not empty.
        // TODO This could also be YES IF I already had 'committed' log entries from my own term. Maybe I should check
        // TODO in on that.
        final boolean entriesFromMyTerm = !newLogEntries.isEmpty();
        final int[] count = {0};
        final boolean[] ackedToClient = {false};

        Futures.addCallback(localLogFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                assert result != null && result;

                processAckAndAct(count, entriesFromMyTerm, ackedToClient, majority, reqs, lastIndexSent);
            }

            @Override
            public void onFailure(Throwable t) {
                // pretty bad.
                LOG.error("{} failed to commit to local log {}", myId, t);
            }
        });

        for (final long peer : peers) {
            if (myId == peer) continue; // dont send myself messages.

            // for each peer, figure out how many "back messages" should I send:
            final long peerNextIdx = this.peersNextIndex.get(peer);

            ArrayList<LogEntry> peerEntries = newLogEntries;
            if (peerNextIdx < firstInList) {
                long moreCount = firstInList - peerNextIdx;
                LOG.debug("{} sending {} more log entires to peer {}", myId, moreCount, peer);

                // TODO check moreCount is reasonable, and available in log. Otherwise do alternative peer catch up
                // TODO alternative peer catchup is by a different process, send message to that then skip sending AppendRpc

                // TODO cache these extra LogEntry objects so we dont recreate too many of them.
                peerEntries = new ArrayList<>((int) (newLogEntries.size() + moreCount));
                for (long i = peerNextIdx; i < firstInList; i++) {
                    LogEntry entry = log.getLogEntry(i);
                    peerEntries.add(entry);
                }
                // make sure the lists splice neatly together.
                assert (peerEntries.get(peerEntries.size()-1).getIndex()+1) == newLogEntries.get(0).getIndex();
                peerEntries.addAll(newLogEntries);
            }

            // catch them up so the next RPC wont over-send old junk.
            peersNextIndex.put(peer, lastIndexSent + 1);

            Raft.AppendEntries msg = msgProto
                    .clone()
                    .addAllEntries(peerEntries)
                    .build();

            // TODO handle failures and notification of the clients.

            RpcRequest request = new RpcRequest(peer, myId, msg);
            AsyncRequest.withOneReply(fiber, sendRpcChannel, request, new Callback<RpcWireReply>() {
                @Override
                public void onMessage(RpcWireReply message) {
                    LOG.debug("{} got a reply {}", myId, message);

                    boolean wasSuccessful = message.getAppendReplyMessage().getSuccess();
                    if (!wasSuccessful) {
                        // This is per Page 7, paragraph 5.  "After a rejection, the leader decrements nextIndex and retries"

                        peersNextIndex.put(peer, peerNextIdx - 1);
                    } else {
                        processAckAndAct(count, entriesFromMyTerm, ackedToClient, majority, reqs, lastIndexSent);
                    }
                }
            });
        }
    }

    private void processAckAndAct(int[] count, boolean entriesFromMyTerm, boolean[] ackedToClient, long majority, ArrayList<IntLogRequest> reqs, long lastIndexSent) {
        // if # of acks > majority, THEN -> this is OK
        // IFF one entry from the current term is also stored on a majority of the servers.
        count[0]++;

        if (entriesFromMyTerm && !ackedToClient[0] && count[0] > majority ) {
            // ok we can deliver notification to the client.
            for (IntLogRequest req : reqs) {
                req.durableNofication.set(true);
            }

            ackedToClient[0] = true;

            // therefore the most recently visible commit is the last one in the batch we sent out.
            lastCommittedIndex = lastIndexSent;
        }
    }

    @FiberOnly
    private void startAppendTimer() {
        appender = fiber.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                sendAppendRPC();
            }
        }, 0, info.electionTimeout()/2, TimeUnit.MILLISECONDS);
    }

    @FiberOnly
    private void stopAppendTimer() {
        if (appender != null) {
            appender.dispose();
            appender = null;
        }
    }

    @FiberOnly
    private void sendAppendRPC() {
        Raft.AppendEntries msg = Raft.AppendEntries.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(myId)
                .setPrevLogIndex(log.getLastIndex())
                .setPrevLogTerm(log.getLastTerm())
                .setCommitIndex(0)
                .build();

        for(Long peer : peers) {
            if (myId == peer) continue; // dont send myself messages.
            RpcRequest request = new RpcRequest(peer, myId, msg);
            AsyncRequest.withOneReply(fiber, sendRpcChannel, request, new Callback<RpcWireReply>() {
                @Override
                public void onMessage(RpcWireReply message) {
                    LOG.debug("{} got a reply {}", myId, message);
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
