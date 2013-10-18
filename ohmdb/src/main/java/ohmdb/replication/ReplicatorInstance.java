package ohmdb.replication;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import ohmdb.replication.rpc.RpcReply;
import ohmdb.replication.rpc.RpcRequest;
import ohmdb.replication.rpc.RpcWireReply;
import ohmdb.replication.rpc.RpcWireRequest;
import ohmdb.util.FiberOnly;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ohmdb.replication.Raft.LogEntry;

/**
 * Single instantation of a raft / log / lease
 */
public class ReplicatorInstance {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatorInstance.class);

    @Override
    public String toString() {
        return "ReplicatorInstance{" +
                "myId=" + myId +
                ", quorumId='" + quorumId + '\'' +
                ", lastCommittedIndex=" + lastCommittedIndex +
                ", myState=" + myState +
                ", currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", lastRPC=" + lastRPC +
                '}';
    }

    public RequestChannel<RpcWireRequest, RpcReply> getIncomingChannel() {
        return incomingChannel;
    }

    private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel;
    private final RequestChannel<RpcWireRequest, RpcReply> incomingChannel = new MemoryRequestChannel<>();
    private final Channel<ReplicatorInstanceStateChange> stateChangeChannel;

    /********** final fields *************/
    private final Fiber fiber;
    private final long myId;
    private final String quorumId;

    private final ImmutableList<Long> peers;

    /****** These next few fields are used when we are a leader *******/
    // this is the next index from our log we need to send to each peer, kept track of on a per-peer basis.
    private HashMap<Long, Long> peersNextIndex;
    // The last succesfully acked message from our peers.  I also keep track of my own acked log messages in here.
    private HashMap<Long, Long> peersLastAckedIndex;
    private long myFirstIndexAsLeader;
    private long lastCommittedIndex;

    public String getQuorumId() {
        return quorumId;
    }

    private static class IntLogRequest {
        public final byte[] datum;
        public final SettableFuture<Long> logNumberNotifation;

        private IntLogRequest(byte[] datum) {
            this.datum = datum;
            this.logNumberNotifation = SettableFuture.create();
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
    private long lastRPC;
    private long myElectionTimeout;
    private Disposable electionChecker;

    private final RaftLogAbstraction log;
    final RaftInformationInterface info;
    final RaftInfoPersistence persister;

    public ReplicatorInstance(final Fiber fiber,
                              final long myId,
                              final String quorumId,
                              List<Long> peers,
                              RaftLogAbstraction log,
                              RaftInformationInterface info,
                              RaftInfoPersistence persister,
                              RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel,
                              final Channel<ReplicatorInstanceStateChange> stateChangeChannel) {
        this.fiber = fiber;
        this.myId = myId;
        this.quorumId = quorumId;
        this.peers = ImmutableList.copyOf(peers);
        this.sendRpcChannel = sendRpcChannel;
        this.log = log;
        this.info = info;
        this.persister = persister;
        this.stateChangeChannel = stateChangeChannel;
        Random r = new Random();
        this.myElectionTimeout = r.nextInt((int) info.electionTimeout()) + info.electionTimeout();
        this.lastRPC = info.currentTimeMillis();

        assert this.peers.contains(this.myId);

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    readPersistentData();
                    // indicate we are running!
                    stateChangeChannel.publish(
                            new ReplicatorInstanceStateChange(ReplicatorInstance.this, Service.State.RUNNING, null));
                } catch (IOException e) {
                    LOG.error("{} {} error during persistent data init {}", quorumId, myId, e);
                    stateChangeChannel.publish(
                            new ReplicatorInstanceStateChange(ReplicatorInstance.this, Service.State.FAILED, e));
                    fiber.dispose(); // kill us forever.
                }
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

        LOG.debug("{} started {} with election timeout {}", myId, this.quorumId, this.myElectionTimeout);
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
    public ListenableFuture<Long> logData(byte[] datum) throws InterruptedException {
        if (!isLeader()) {
            LOG.debug("{} attempted to logData on a non-leader", myId);
            return null;
        }

        IntLogRequest req = new IntLogRequest(datum);
        logRequests.put(req);

        // TODO return the durable notification future?
        return req.logNumberNotifation;
    }

    @FiberOnly
    private void readPersistentData() throws IOException {
        currentTerm = persister.readCurrentTerm(quorumId);
        votedFor = persister.readVotedFor(quorumId);
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
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
            RpcReply reply = new RpcReply(m);
            message.reply(reply);
            return;
        }

        // 2. if term > currentTerm, currentTerm <- term
        if (msg.getTerm() > currentTerm) {
            LOG.debug("{} requestVote rpc, pushing forward currentTerm {} to {}", myId, currentTerm, msg.getTerm());
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
                .setTerm(currentTerm)
                .setVoteGranted(vote)
                .build();
        RpcReply reply = new RpcReply(m);
        message.reply(reply);
    }

    @FiberOnly
    private void doAppendMessage(final Request<RpcWireRequest, RpcReply> message) {
        Raft.AppendEntries msg = message.getRequest().getAppendMessage();

        // 1. return if term < currentTerm (sec 5.1)
        if (msg.getTerm() < currentTerm) {
            // TODO is this the correct message reply?
            Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();

            RpcReply reply = new RpcReply(m);
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
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .build();

            RpcReply reply = new RpcReply(m);
            message.reply(reply);
            return;
        }

        // 5. return failure if log doesn't contain an entry at
        // prevLogIndex who's term matches prevLogTerm (sec 5.3)
        // if msgPrevLogIndex == 0 -> special case of starting the log!
        long msgPrevLogIndex = msg.getPrevLogIndex();
        long msgPrevLogTerm = msg.getPrevLogTerm();
        if (msgPrevLogIndex != 0 && log.getLogTerm(msgPrevLogIndex) != msgPrevLogTerm) {
            Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .setMyLastLogEntry(log.getLastIndex())
                    .build();

            RpcReply reply = new RpcReply(m);
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
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();

                RpcReply reply = new RpcReply(m);
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
                        .setTerm(currentTerm)
                        .setSuccess(true)
                        .build();

                RpcReply reply = new RpcReply(m);
                message.reply(reply);
            }

            @Override
            public void onFailure(Throwable t) {
                // TODO better error reporting. A log commit failure will e a serious issue.
                Raft.AppendEntriesReply m = Raft.AppendEntriesReply.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();

                RpcReply reply = new RpcReply(m);
                message.reply(reply);
            }
        });
    }

    @FiberOnly
    private void checkOnElection() {
        if (myState == State.LEADER) {
            LOG.trace("{} leader during election check.", myId);
            return;
        }

        if (lastRPC + this.myElectionTimeout < info.currentTimeMillis()) {
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
            RpcRequest req = new RpcRequest(peer, myId, quorumId, msg);
            AsyncRequest.withOneReply(fiber, sendRpcChannel, req, new Callback<RpcWireReply>() {
                @FiberOnly
                @Override
                public void onMessage(RpcWireReply message) {
                    handleElectionReply0(message, termBeingVotedFor, votes, majority);
                }
            }, 1, TimeUnit.SECONDS, new RequestVoteTimeout(req, termBeingVotedFor, votes, majority));
        }
    }

    private class RequestVoteTimeout implements Runnable {
        public final RpcRequest request;
        public final long termBeingVotedFor;
        public final List<Long> votes;
        public final int majority;

        @Override
        public String toString() {
            return "RequestVoteTimeout{" +
                    "request=" + request +
                    ", termBeingVotedFor=" + termBeingVotedFor +
                    ", votes=" + votes +
                    ", majority=" + majority +
                    '}';
        }

        private RequestVoteTimeout(RpcRequest request, long termBeingVotedFor, List<Long> votes, int majority) {
            this.request = request;
            this.termBeingVotedFor = termBeingVotedFor;
            this.votes = votes;
            this.majority = majority;
        }

        @Override
        public void run() {
            // If we are no longer a candidate, retrying RequestVote is pointless.
            if (myState != State.CANDIDATE)
                return;

            // Also if the term goes forward somehow, this is also out of date, and drop it.
            if (currentTerm > termBeingVotedFor)
                return;

            // Note we are using 'this' as the recursive timeout.
            AsyncRequest.withOneReply(fiber, sendRpcChannel, request, new Callback<RpcWireReply>() {
                @Override
                public void onMessage(RpcWireReply message) {
                    handleElectionReply0(message, termBeingVotedFor, votes, majority);
                }
            }, 1, TimeUnit.SECONDS, this);
        }
    }

    // RPC callback for timeouts
    private void handleElectionReply0(RpcWireReply message, long termBeingVotedFor, List<Long> votes, int majority) {
        // if current term has advanced, these replies are stale and should be ignored:

        if (currentTerm > termBeingVotedFor) {
            LOG.warn("{} election reply from {}, but currentTerm {} > vote term {}", myId, message.from,
                    currentTerm, termBeingVotedFor);
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


    //// Leader timer stuff below
    private Disposable queueConsumer;
    private Disposable appender = null;

    @FiberOnly
    private void haltLeader() {
        myState = State.FOLLOWER;

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

        peersLastAckedIndex = new HashMap<>(peers.size());
        peersNextIndex = new HashMap<>(peers.size()-1);
        for (long peer : peers) {
            if (peer == myId) continue;

            peersNextIndex.put(peer, myNextLog);
        }

        // none so far!
        myFirstIndexAsLeader = 0;
        lastCommittedIndex = 0; // unknown as of yet

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

        LOG.trace("{} queue consuming", myId);
        while (logRequests.peek() != null) {
            reqs.add(logRequests.poll());
        }

        LOG.trace("{} {} queue items to commit", myId, reqs.size());

        final long firstInList = log.getLastIndex()+1;
        long idAssigner = firstInList;

        // Get these now BEFORE the log append call.
        long logLastIndex = log.getLastIndex();
        long logLastTerm = log.getLastTerm();

        // Build the log entries:
        ArrayList <LogEntry> newLogEntries = new ArrayList<>(reqs.size());
        for (IntLogRequest logReq : reqs) {
            LogEntry entry = LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setIndex(idAssigner)
                    .setData(ByteString.copyFrom(logReq.datum))
                    .build();
            newLogEntries.add(entry);

            if (myFirstIndexAsLeader == 0) {
                myFirstIndexAsLeader = idAssigner;
                LOG.debug("{} my first index as leader is: {}", myId, myFirstIndexAsLeader);
            }

            // let the client know what our id is
            logReq.logNumberNotifation.set(idAssigner);

            idAssigner ++;
        }

        // Should throw immediately if there was a basic validation error.
        final ListenableFuture<Boolean> localLogFuture;
        if (!newLogEntries.isEmpty()) {
            localLogFuture = log.logEntries(newLogEntries);
        } else {
            localLogFuture = null;
        }

        Raft.AppendEntries.Builder msgProto = Raft.AppendEntries.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(myId)
                .setPrevLogIndex(logLastIndex)
                .setPrevLogTerm(logLastTerm)
                .setCommitIndex(this.lastCommittedIndex);

        // TODO remove one of these i think.
        final long largestIndexInBatch = idAssigner - 1;
        final long lastIndexSent = log.getLastIndex();
        assert lastIndexSent == largestIndexInBatch;

        // What a majority means at this moment (in case of reconfigures)
        final long majority = calculateMajority(peers.size());

        if (localLogFuture != null)
            Futures.addCallback(localLogFuture, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    assert result != null && result;

                    peersLastAckedIndex.put(myId, lastIndexSent);
                    calculateLastVisible(majority, lastIndexSent);
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

                // TODO allow for smaller 'catch up' messages so we dont try to create a 400GB sized message.

                // TODO cache these extra LogEntry objects so we dont recreate too many of them.
                peerEntries = new ArrayList<>((int) (newLogEntries.size() + moreCount));
                for (long i = peerNextIdx; i < firstInList; i++) {
                    LogEntry entry = log.getLogEntry(i);
                    peerEntries.add(entry);
                }
                // TODO make sure the lists splice neatly together. The check below fails when newLogEntries is empty
                //assert (peerEntries.get(peerEntries.size()-1).getIndex()+1) == newLogEntries.get(0).getIndex();
                peerEntries.addAll(newLogEntries);
            }

            // catch them up so the next RPC wont over-send old junk.
            peersNextIndex.put(peer, lastIndexSent + 1);

            Raft.AppendEntries msg = msgProto
                    .clone()
                    .addAllEntries(peerEntries)
                    .build();

            RpcRequest request = new RpcRequest(peer, myId, quorumId, msg);
            AsyncRequest.withOneReply(fiber, sendRpcChannel, request, new Callback<RpcWireReply>() {
                @Override
                public void onMessage(RpcWireReply message) {
                    LOG.trace("{} got a reply {}", myId, message);

                    boolean wasSuccessful = message.getAppendReplyMessage().getSuccess();
                    if (!wasSuccessful) {
                        // This is per Page 7, paragraph 5.  "After a rejection, the leader decrements nextIndex and retries"
                        if (message.getAppendReplyMessage().hasMyLastLogEntry()) {
                            peersNextIndex.put(peer, message.getAppendReplyMessage().getMyLastLogEntry());
                        } else {
                            peersNextIndex.put(peer, peerNextIdx - 1);
                        }
                    } else {
                        // we have been successfully acked up to this point.
                        LOG.trace("{} peer {} acked for {}", myId, peer, lastIndexSent);
                        peersLastAckedIndex.put(peer, lastIndexSent);

                        calculateLastVisible(majority, lastIndexSent);
                    }
                }
            }, 5, TimeUnit.SECONDS, new Runnable() {
                        @Override
                        public void run() {
                            LOG.trace("{} peer {} timed out", myId, peer);
                            // Do nothing -> let next timeout handle things.
                            // This timeout exists just so that we can cancel and clean up stuff in jetlang.
                        }
                    }
            );
        }
    }

    private void calculateLastVisible(long majority, long lastIndexSent) {
        if (lastIndexSent == lastCommittedIndex) return; //skip null check basically

        HashMap <Long,Integer> bucket = new HashMap<>();
        for (long lastAcked : peersLastAckedIndex.values()) {
            Integer p = bucket.get(lastAcked);
            if (p == null)
                bucket.put(lastAcked, 1);
            else
                bucket.put(lastAcked, p+1);
        }

        long mostAcked = 0;
        for (Map.Entry<Long,Integer> e : bucket.entrySet()) {
            if (e.getValue() > majority) {
                if (mostAcked != 0) {
                    LOG.warn("{} strange, found more than 1 'most acked' entry: {} and {}", myId, mostAcked, e.getKey());
                }
                mostAcked = e.getKey();
            }
        }
        if (mostAcked == 0) return;
        if (myFirstIndexAsLeader == 0) return; // cant declare new visible yet until we have a first index as the leader.

        if (mostAcked < myFirstIndexAsLeader) {
            LOG.warn("{} Found most-acked entry {} but my first index as leader was {}, cant declare visible yet", myId, mostAcked, myFirstIndexAsLeader);
            return;
        }

        if (mostAcked < lastCommittedIndex) {
            LOG.warn("{} weird mostAcked {} is smaller than lastCommittedIndex {}", myId, mostAcked, lastCommittedIndex);
            return;
        }
        if (mostAcked == lastCommittedIndex) {
            return ;
        }
        this.lastCommittedIndex = mostAcked;
        LOG.info("{} discovered new visible entry {}", myId, lastCommittedIndex);

        // TODO take action and notify clients (pending new system frameworks)
    }

    private void setVotedFor(long votedFor) {
        try {
            persister.writeCurrentTermAndVotedFor(quorumId, currentTerm, votedFor);
        } catch (IOException e) {
            stateChangeChannel.publish(new ReplicatorInstanceStateChange(this, Service.State.FAILED, e));
            fiber.dispose();
        }
        //persister.writeVotedFor(votedFor);

        this.votedFor = votedFor;
    }

    private void setCurrentTerm(long newTerm) {
        //persister.writeCurrentTerm(newTerm);
        try {
            persister.writeCurrentTermAndVotedFor(quorumId, newTerm, 0);
        } catch (IOException e) {
            stateChangeChannel.publish(new ReplicatorInstanceStateChange(this, Service.State.FAILED, e));
            fiber.dispose();
        }

        this.currentTerm = newTerm;
        this.votedFor = 0;
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
