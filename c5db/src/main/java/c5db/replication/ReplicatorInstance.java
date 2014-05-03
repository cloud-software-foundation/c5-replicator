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
import c5db.log.ReplicatorLog;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.AppendEntriesReply;
import c5db.replication.generated.LogEntry;
import c5db.replication.generated.RequestVote;
import c5db.replication.generated.RequestVoteReply;
import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import c5db.util.FiberOnly;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Single instantiation of a replicator / log / lease. This implementation's logic is based on the
 * RAFT algorithm (see <a href="http://raftconsensus.github.io/">http://raftconsensus.github.io/</a>.
 * <p>
 * A ReplicatorInstance handles the consensus and replication for a single quorum, and communicates
 * with the log package via {@link c5db.log.ReplicatorLog}.
 */
public class ReplicatorInstance implements Replicator {
  @Override
  public String toString() {
    return "ReplicatorInstance{" +
        "myId=" + myId +
        ", quorumId='" + quorumId + '\'' +
        ", currentConfig='" + currentConfig + '\'' +
        ", lastCommittedIndex=" + lastCommittedIndex +
        ", myState=" + myState +
        ", currentTerm=" + currentTerm +
        ", votedFor=" + votedFor +
        ", lastRPC=" + lastRPC +
        '}';
  }

  private final MemoryChannel<State> stateMemoryChannel = new MemoryChannel<>();

  public RequestChannel<RpcWireRequest, RpcReply> getIncomingChannel() {
    return incomingChannel;
  }

  private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel;
  private final RequestChannel<RpcWireRequest, RpcReply> incomingChannel = new MemoryRequestChannel<>();
  private final Channel<ReplicatorInstanceEvent> stateChangeChannel;
  private final Channel<IndexCommitNotice> commitNoticeChannel;

  /**
   * ******* final fields ************
   */
  private final Fiber fiber;
  private final long myId;
  private final String quorumId;
  private final Logger logger;

  /**
   * *** These next few fields are used when we are a leader ******
   */
  // this is the next index from our log we need to send to each peer, kept track of on a per-peer basis.
  private Map<Long, Long> peersNextIndex;
  // The last successfully acked message from our peers.  I also keep track of my own acked log messages in here.
  private Map<Long, Long> peersLastAckedIndex;
  private long myFirstIndexAsLeader;
  private long lastCommittedIndex;

  private static class IntLogRequest {
    public final List<ByteBuffer> data;
    public final SettableFuture<Long> logNumberNotification;

    private IntLogRequest(List<ByteBuffer> data) {
      this.data = data;
      this.logNumberNotification = SettableFuture.create();
    }
  }

  private final BlockingQueue<IntLogRequest> logRequests = new ArrayBlockingQueue<>(100);

  State myState = State.FOLLOWER;

  // In theory these are persistent:
  long currentTerm;
  long votedFor;
  private QuorumConfiguration currentConfig;

  // Election timers, etc.
  private long lastRPC;
  private long myElectionTimeout;
  private long whosLeader = 0;

  @SuppressWarnings("UnusedDeclaration")
  private Disposable electionChecker;

  private final ReplicatorLog log;
  final ReplicatorInformationInterface info;
  final ReplicatorInfoPersistence persister;

  public ReplicatorInstance(final Fiber fiber,
                            final long myId,
                            final String quorumId,
                            List<Long> peers,
                            ReplicatorLog log,
                            ReplicatorInformationInterface info,
                            ReplicatorInfoPersistence persister,
                            RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel,
                            final Channel<ReplicatorInstanceEvent> stateChangeChannel,
                            final Channel<IndexCommitNotice> commitNoticeChannel) {
    this.fiber = fiber;
    this.myId = myId;
    this.quorumId = quorumId;
    this.logger = getNewLogger();
    this.currentConfig = QuorumConfiguration.of(peers);
    this.sendRpcChannel = sendRpcChannel;
    this.log = log;
    this.info = info;
    this.persister = persister;
    this.stateChangeChannel = stateChangeChannel;
    this.commitNoticeChannel = commitNoticeChannel;
    Random r = new Random();
    this.myElectionTimeout = r.nextInt((int) info.electionTimeout()) + info.electionTimeout();
    this.lastRPC = info.currentTimeMillis();
    this.lastCommittedIndex = 0;

    assert currentConfig.allPeers().contains(this.myId);

    fiber.execute(() -> {
      try {
        readPersistentData();
        // indicate we are running!
        stateChangeChannel.publish(
            new ReplicatorInstanceEvent(
                ReplicatorInstanceEvent.EventType.QUORUM_START,
                ReplicatorInstance.this,
                0,
                info.currentTimeMillis(),
                null)
        );
      } catch (IOException e) {
        logger.error("error during persistent data init", e);
        failReplicatorInstance(e);
      }
    });

    incomingChannel.subscribe(fiber, this::onIncomingMessage);

    electionChecker = fiber.scheduleWithFixedDelay(this::checkOnElection, info.electionCheckRate(),
        info.electionCheckRate(), TimeUnit.MILLISECONDS);

    logger.debug("primed");
  }

  /**
   * Initialize object into the specified state, for testing purposes
   */
  ReplicatorInstance(final Fiber fiber,
                     final long myId,
                     final String quorumId,
                     List<Long> peers,
                     ReplicatorLog log,
                     ReplicatorInformationInterface info,
                     ReplicatorInfoPersistence persister,
                     RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel,
                     final Channel<ReplicatorInstanceEvent> stateChangeChannel,
                     final Channel<IndexCommitNotice> commitNoticeChannel,
                     long term,
                     State state,
                     long lastCommittedIndex,
                     long leaderId,
                     long votedFor) {

    this.fiber = fiber;
    this.myId = myId;
    this.quorumId = quorumId;
    this.logger = getNewLogger();
    this.currentConfig = QuorumConfiguration.of(peers);
    this.sendRpcChannel = sendRpcChannel;
    this.log = log;
    this.info = info;
    this.persister = persister;
    this.stateChangeChannel = stateChangeChannel;
    this.commitNoticeChannel = commitNoticeChannel;
    this.myElectionTimeout = info.electionTimeout();
    this.lastRPC = info.currentTimeMillis();

    assert currentConfig.allPeers().contains(myId);
    assert votedFor == 0 || currentConfig.allPeers().contains(votedFor);
    assert leaderId == 0 || currentConfig.allPeers().contains(leaderId);

    incomingChannel.subscribe(fiber, this::onIncomingMessage);
    electionChecker = fiber.scheduleWithFixedDelay(this::checkOnElection,
        info.electionCheckRate(), info.electionCheckRate(), TimeUnit.MILLISECONDS);

    logger.debug("primed");

    this.currentTerm = term;
    this.myState = state;
    this.lastCommittedIndex = lastCommittedIndex;
    this.whosLeader = leaderId;
    this.votedFor = votedFor;

    try {
      persister.writeCurrentTermAndVotedFor(quorumId, currentTerm, votedFor);
    } catch (IOException e) {
      failReplicatorInstance(e);
    }

    if (state == State.LEADER) {
      becomeLeader();
    }
  }

  // public API:

  @Override
  public String getQuorumId() {
    return quorumId;
  }

  @Override
  public ListenableFuture<Long> logData(List<ByteBuffer> data) throws InterruptedException {
    if (!isLeader()) {
      logger.debug("attempted to logData on a non-leader");
      return null;
    }

    IntLogRequest req = new IntLogRequest(data);
    logRequests.put(req);

    // TODO return the durable notification future?
    return req.logNumberNotification;
  }

  @Override
  public long getId() {
    return myId;
  }

  @Override
  public boolean isLeader() {
    return myState == State.LEADER;
  }

  @Override
  public void start() {
    logger.debug("started {} with election timeout {}", this.quorumId, this.myElectionTimeout);
    fiber.start();
  }

  public void dispose() {
    fiber.dispose();
  }

  @Override
  public Channel<State> getStateChannel() {
    return stateMemoryChannel;
  }


  void failReplicatorInstance(Throwable e) {
    stateChangeChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.QUORUM_FAILURE,
            this,
            0,
            info.currentTimeMillis(),
            e)
    );
    fiber.dispose(); // kill us forever.
  }

  @FiberOnly
  private void readPersistentData() throws IOException {
    currentTerm = persister.readCurrentTerm(quorumId);
    votedFor = persister.readVotedFor(quorumId);
  }

  @FiberOnly
  private void onIncomingMessage(Request<RpcWireRequest, RpcReply> message) {
    try {
      RpcWireRequest req = message.getRequest();
      if (req.isRequestVoteMessage()) {
        doRequestVote(message);

      } else if (req.isAppendMessage()) {
        doAppendMessage(message);

      } else {
        logger.warn("got a message of protobuf type I dont know: {}", req);
      }
    } catch (Exception e) {
      logger.error("exception while processing message {}: {}", message, e);
      throw e;
    }
  }

  @FiberOnly
  private void doRequestVote(Request<RpcWireRequest, RpcReply> message) {
    RequestVote msg = message.getRequest().getRequestVoteMessage();

    // 1. Return if term < currentTerm (sec 5.1)
    if (msg.getTerm() < currentTerm) {
      RequestVoteReply m = new RequestVoteReply(currentTerm, false);
      RpcReply reply = new RpcReply(m);
      message.reply(reply);
      return;
    }

    // 2. if term > currentTerm, currentTerm <- term
    if (msg.getTerm() > currentTerm) {
      logger.debug("RequestVote rpc, pushing forward currentTerm {} to {}", currentTerm, msg.getTerm());
      setCurrentTerm(msg.getTerm());

      // 2a. Step down if candidate or leader.
      if (myState != State.FOLLOWER) {
        logger.debug("stepping down to follower, currentTerm: {}", currentTerm);

        becomeFollower();
      }
    }

    // 3. if votedFor is null (0), or candidateId, and candidate's log
    // is at least as complete as local log (sec 5.2, 5.4), grant vote
    // and reset election timeout.

    boolean vote = false;
    if ((log.getLastTerm() <= msg.getLastLogTerm())
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

    logger.debug("sending vote reply to {} vote = {}, voted = {}", message.getRequest().from, votedFor, vote);
    RequestVoteReply m = new RequestVoteReply(currentTerm, vote);
    RpcReply reply = new RpcReply(m);
    message.reply(reply);
  }

  @FiberOnly
  private void doAppendMessage(final Request<RpcWireRequest, RpcReply> request) {
    final AppendEntries appendMessage = request.getRequest().getAppendMessage();

    // 1. return if term < currentTerm (sec 5.1)
    if (appendMessage.getTerm() < currentTerm) {
      // TODO is this the correct message reply?
      AppendEntriesReply m = new AppendEntriesReply(currentTerm, false, 0);
      RpcReply reply = new RpcReply(m);
      request.reply(reply);
      return;
    }

    // 2. if term > currentTerm, set it (sec 5.1)
    if (appendMessage.getTerm() > currentTerm) {
      setCurrentTerm(appendMessage.getTerm());
    }

    // 3. Step down if we are a leader or a candidate (sec 5.2, 5.5)
    if (myState != State.FOLLOWER) {
      becomeFollower();
    }

    // 4. reset election timeout
    lastRPC = info.currentTimeMillis();

    long theLeader = appendMessage.getLeaderId();
    if (whosLeader != theLeader) {
      acknowledgeNewLeader(theLeader);
    }

    // 5. return failure if log doesn't contain an entry at
    // prevLogIndex who's term matches prevLogTerm (sec 5.3)
    // if msgPrevLogIndex == 0 -> special case of starting the log!
    long msgPrevLogIndex = appendMessage.getPrevLogIndex();
    long msgPrevLogTerm = appendMessage.getPrevLogTerm();
    if (msgPrevLogIndex != 0 && log.getLogTerm(msgPrevLogIndex) != msgPrevLogTerm) {
      AppendEntriesReply m = new AppendEntriesReply(currentTerm, false, log.getLastIndex());
      RpcReply reply = new RpcReply(m);
      request.reply(reply);
      return;
    }

    if (appendMessage.getEntriesList().isEmpty()) {
      AppendEntriesReply m = new AppendEntriesReply(currentTerm, true, 0);
      RpcReply reply = new RpcReply(m);
      request.reply(reply);
      long newCommitIndex = Math.min(appendMessage.getCommitIndex(), log.getLastIndex());
      setLastCommittedIndex(newCommitIndex);
      return;
    }

    // 6. if existing entries conflict with new entries, delete all
    // existing entries starting with first conflicting entry (sec 5.3)
    // nb: The process in which we fix the local log may involve a async log operation, so that is entirely
    // hidden up in this future.  Note that the process can fail, so we handle that as well.
    ListenableFuture<ArrayList<LogEntry>> entriesToCommitFuture = validateAndFixLocalLog(appendMessage);
    // TODO this method of placing callbacks on the replicator's fiber can cause race conditions
    // TODO since later-received messages can be handled prior to the callback on earlier-received messages
    Futures.addCallback(entriesToCommitFuture, new FutureCallback<ArrayList<LogEntry>>() {
      @Override
      public void onSuccess(ArrayList<LogEntry> entriesToCommit) {

        // 7. Append any new entries not already in the log.
        ListenableFuture<Boolean> logCommitNotification = log.logEntries(entriesToCommit);

        // 8. apply newly committed entries to state machine

        // wait for the log to commit before returning message.  But do so async.
        Futures.addCallback(logCommitNotification, new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(Boolean result) {
            AppendEntriesReply m = new AppendEntriesReply(currentTerm, true, 0);
            RpcReply reply = new RpcReply(m);
            request.reply(reply);

            // Notify and mark the last committed index.
            long newCommitIndex = Math.min(appendMessage.getCommitIndex(), log.getLastIndex());
            setLastCommittedIndex(newCommitIndex);
          }

          @Override
          public void onFailure(Throwable t) {
            // TODO A log commit failure is probably a fatal error. Quit the instance?
            // TODO better error reporting. A log commit failure will be a serious issue.
            logger.error("failure appending new entries to log", t);
            AppendEntriesReply m = new AppendEntriesReply(currentTerm, false, 0);

            RpcReply reply = new RpcReply(m);
            request.reply(reply);
          }
        }, fiber);

      }

      @Override
      public void onFailure(Throwable t) {
        logger.error("failure validating and fixing local log", t);
        AppendEntriesReply m = new AppendEntriesReply(currentTerm, false, 0);
        RpcReply reply = new RpcReply(m);
        request.reply(reply);
      }
    }, fiber);
  }

  @FiberOnly
  private void acknowledgeNewLeader(long theLeader) {
    logger.debug("discovered new leader: {}", theLeader);
    whosLeader = theLeader;

    stateChangeChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.LEADER_ELECTED,
            this,
            whosLeader,
            info.currentTimeMillis(),
            null)
    );
  }

  private ListenableFuture<ArrayList<LogEntry>> validateAndFixLocalLog(AppendEntries appendMessage) {
    final SettableFuture<ArrayList<LogEntry>> future = SettableFuture.create();

    validateAndFixLocalLog0(appendMessage, future);
    return future;
  }

  private void validateAndFixLocalLog0(final AppendEntries appendMessage,
                                       final SettableFuture<ArrayList<LogEntry>> future) {

    // 6. if existing entries conflict with new entries, delete all
    // existing entries starting with first conflicting entry (sec 5.3)

    long nextIndex = log.getLastIndex() + 1;

    List<LogEntry> entries = appendMessage.getEntriesList();
    ArrayList<LogEntry> entriesToCommit = new ArrayList<>(entries.size());
    for (LogEntry entry : entries) {
      long entryIndex = entry.getIndex();

      if (entryIndex == nextIndex) {
        logger.debug("new log entry for idx {} term {}", entryIndex, entry.getTerm());

        entriesToCommit.add(entry);

        nextIndex++;
        continue;
      }

      if (entryIndex > nextIndex) {
        // ok this entry is still beyond the LAST entry, so we have a problem:
        logger.error("log entry missing, I expected {} and the next in the message is {}",
            nextIndex, entryIndex);

        future.setException(new Exception("Log entry missing"));
        return;
      }

      // at this point entryIndex should be <= log.getLastIndex
      assert entryIndex < nextIndex;

      if (log.getLogTerm(entryIndex) != entry.getTerm()) {
        // This is generally expected to be fairly uncommon.  To prevent busywaiting on the truncate,
        // we basically just redo some work (that ideally shouldn't be too expensive).

        // So after this point, we basically return immediately, with a callback schedule.

        // conflict:
        logger.debug("log conflict at idx {} my term: {} term from leader: {}, truncating log after this point",
            entryIndex, log.getLogTerm(entryIndex), entry.getTerm());

        // delete this and all subsequent entries:
        ListenableFuture<Boolean> truncateResult = log.truncateLog(entryIndex);
        Futures.addCallback(truncateResult, new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(Boolean ignored) {
            // Recurse, which involved a little redo work, but at makes this code easier to reason about.
            validateAndFixLocalLog0(appendMessage, future);
          }

          @Override
          public void onFailure(Throwable t) {
            failReplicatorInstance(t);
            future.setException(t); // TODO determine if this is the proper thing to do here?
          }
        }, fiber);

        return;
      } //else {
      // this log entry did NOT conflict we dont need to re-commit this entry.
      //}
    }
    future.set(entriesToCommit);
  }

  @FiberOnly
  private void checkOnElection() {
    if (myState == State.LEADER) {
      logger.trace("leader during election check.");
      return;
    }

    if (lastRPC + this.myElectionTimeout < info.currentTimeMillis()) {
      logger.trace("timed out checking on election, try new election");
      doElection();
    }
  }

  @FiberOnly
  private void doElection() {
    stateChangeChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.ELECTION_TIMEOUT,
            this,
            0,
            info.currentTimeMillis(),
            null)
    );

    // Start new election "timer".
    lastRPC = info.currentTimeMillis();
    // increment term.
    setCurrentTerm(currentTerm + 1);
    myState = State.CANDIDATE;

    RequestVote msg = new RequestVote(currentTerm, myId, log.getLastIndex(), log.getLastTerm());

    logger.debug("starting election for currentTerm: {}", currentTerm);

    final long termBeingVotedFor = currentTerm;
    final Set<Long> votes = new HashSet<>();
    for (long peer : currentConfig.allPeers()) {
      RpcRequest req = new RpcRequest(peer, myId, quorumId, msg);
      AsyncRequest.withOneReply(fiber, sendRpcChannel, req,
          message -> handleElectionReply0(message, termBeingVotedFor, votes),
          1, TimeUnit.SECONDS, new RequestVoteTimeout(req, termBeingVotedFor, votes));
    }
  }

  private class RequestVoteTimeout implements Runnable {
    public final RpcRequest request;
    public final long termBeingVotedFor;
    public final Set<Long> votes;

    @Override
    public String toString() {
      return "RequestVoteTimeout{" +
          "request=" + request +
          ", termBeingVotedFor=" + termBeingVotedFor +
          ", votes=" + votes +
          '}';
    }

    private RequestVoteTimeout(RpcRequest request, long termBeingVotedFor, Set<Long> votes) {
      this.request = request;
      this.termBeingVotedFor = termBeingVotedFor;
      this.votes = votes;
    }

    @Override
    public void run() {
      // If we are no longer a candidate, retrying RequestVote is pointless.
      if (myState != State.CANDIDATE) {
        return;
      }

      // Also if the term goes forward somehow, this is also out of date, and drop it.
      if (currentTerm > termBeingVotedFor) {
        logger.trace("request vote timeout, current term has moved on, abandoning this request");
        return;
      }

      logger.trace("request vote timeout to {}, resending RPC", request.to);

      // Note we are using 'this' as the recursive timeout.
      AsyncRequest.withOneReply(fiber, sendRpcChannel, request,
          message -> handleElectionReply0(message, termBeingVotedFor, votes),
          1, TimeUnit.SECONDS, this);
    }
  }

  // RPC callback for timeouts
  private void handleElectionReply0(RpcWireReply message, long termBeingVotedFor, Set<Long> votes) {
    // if current term has advanced, these replies are stale and should be ignored:

    if (message == null) {
      logger.warn("got a NULL message reply, that's unfortunate");
      return;
    }

    if (currentTerm > termBeingVotedFor) {
      logger.warn("election reply from {}, but currentTerm {} > vote term {}", message.from,
          currentTerm, termBeingVotedFor);
      return;
    }

    // if we are no longer a Candidate, election was over, these replies are stale.
    if (myState != State.CANDIDATE) {
      // we became not, ignore
      logger.warn("election reply from {} ignored -> in state {}", message.from, myState);
      return;
    }

    RequestVoteReply reply = message.getRequestVoteReplyMessage();

    if (reply.getTerm() > currentTerm) {
      logger.warn("election reply from {}, but term {} was not my term {}, updating currentTerm",
          message.from, reply.getTerm(), currentTerm);

      setCurrentTerm(reply.getTerm());
      return;
    } else if (reply.getTerm() < currentTerm) {
      // huh weird.
      logger.warn("election reply from {}, their term {} < currentTerm {}",
          message.from, reply.getTerm(), currentTerm);
    }

    // did you vote for me?
    if (reply.getVoteGranted()) {
      // yes!
      votes.add(message.from);
    }

    if (votesConstituteMajority(votes)) {
      becomeLeader();
    }
  }

  private boolean votesConstituteMajority(Set<Long> votes) {
    return currentConfig.setContainsMajority(votes);
  }


  //// Leader timer stuff below
  private Disposable queueConsumer;

  @FiberOnly
  private void becomeFollower() {
    boolean wasLeader = myState == State.LEADER;
    myState = State.FOLLOWER;

    if (wasLeader) {
      stateChangeChannel.publish(
          new ReplicatorInstanceEvent(
              ReplicatorInstanceEvent.EventType.LEADER_DEPOSED,
              this,
              0,
              info.currentTimeMillis(),
              null));
    }

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
    logger.warn("I AM THE LEADER NOW, commence AppendEntries RPCs term = {}", currentTerm);

    myState = State.LEADER;
    stateMemoryChannel.publish(State.LEADER);

    // Page 7, para 5
    long myNextLog = log.getLastIndex() + 1;

    final int numPeers = numPeers();
    peersLastAckedIndex = new HashMap<>(numPeers);
    peersNextIndex = new HashMap<>(numPeers - 1);
    for (long peer : allPeersExceptMe()) {
      peersNextIndex.put(peer, myNextLog);
    }

    // none so far!
    myFirstIndexAsLeader = 0;


    stateChangeChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.LEADER_ELECTED,
            this,
            myId,
            info.currentTimeMillis(),
            null)
    );


    startQueueConsumer();
  }

  @FiberOnly
  private void startQueueConsumer() {
    queueConsumer = fiber.scheduleAtFixedRate(() -> {
      try {
        consumeQueue();
      } catch (Throwable t) {
        failReplicatorInstance(t);
      }
    }, 0, info.groupCommitDelay(), TimeUnit.MILLISECONDS);
  }

  @FiberOnly
  private void consumeQueue() {
    // retrieve as many items as possible. send rpc.
    final List<IntLogRequest> reqs = new ArrayList<>();

    logger.trace("queue consuming");
    while (logRequests.peek() != null) {
      reqs.add(logRequests.poll());
    }

    logger.trace("{} queue items to commit", reqs.size());

    final long firstIndexInList = log.getLastIndex() + 1;
    final long lastIndexInList = firstIndexInList + reqs.size() - 1;

    List<LogEntry> newLogEntries = createLogEntriesFromIntRequests(reqs, firstIndexInList);
    leaderLogNewEntries(newLogEntries, lastIndexInList);

    assert lastIndexInList == log.getLastIndex();

    for (final long peer : allPeersExceptMe()) {

      // for each peer, figure out how many "back messages" should I send:
      final long peerNextIdx = this.peersNextIndex.get(peer);

      if (peerNextIdx < firstIndexInList) {
        final long moreCount = firstIndexInList - peerNextIdx;
        logger.debug("sending {} more log entries to peer {}", moreCount, peer);

        // TODO check moreCount is reasonable, and available in log. Otherwise do alternative peer catch up
        // TODO alternative peer catchup is by a different process, send message to that then skip sending AppendRpc

        // TODO allow for smaller 'catch up' messages so we dont try to create a 400GB sized message.

        // TODO cache these extra LogEntry objects so we dont recreate too many of them.

        ListenableFuture<List<LogEntry>> peerEntriesFuture = log.getLogEntries(peerNextIdx, firstIndexInList);

        Futures.addCallback(peerEntriesFuture, new FutureCallback<List<LogEntry>>() {
          @Override
          public void onSuccess(List<LogEntry> entriesFromLog) {
            // TODO make sure the lists splice neatly together.
            assert entriesFromLog.size() == moreCount;
            if (peerNextIdx != peersNextIndex.get(peer) ||
                myState != State.LEADER) {
              // These were the same when we started checking the log, but they're not now -- that means
              // things happened while the log was retrieving, so discard this result. This is safe because
              // the next (or concurrent) run of consumeQueue has better information.
              return;
            }

            List<LogEntry> entriesToAppend = new ArrayList<>((int) (newLogEntries.size() + moreCount));
            entriesToAppend.addAll(entriesFromLog);
            entriesToAppend.addAll(newLogEntries);
            sendAppendEntries(peer, peerNextIdx, lastIndexInList, entriesToAppend);
          }

          @Override
          public void onFailure(Throwable throwable) {
            // Failed to retrieve from local log
            // TODO is this situation ever recoverable?
            failReplicatorInstance(throwable);
          }
        }, fiber);
      } else {
        sendAppendEntries(peer, peerNextIdx, lastIndexInList, newLogEntries);
      }
    }
  }

  @FiberOnly
  private List<LogEntry> createLogEntriesFromIntRequests(List<IntLogRequest> requests, long firstEntryIndex) {
    long idAssigner = firstEntryIndex;

    // Build the log entries:
    List<LogEntry> newLogEntries = new ArrayList<>(requests.size());
    for (IntLogRequest logReq : requests) {
      LogEntry entry = new LogEntry(currentTerm, idAssigner, logReq.data);
      newLogEntries.add(entry);

      if (myFirstIndexAsLeader == 0) {
        myFirstIndexAsLeader = idAssigner;
        logger.debug("my first index as leader is: {}", myFirstIndexAsLeader);
      }

      // let the client know what our id is
      logReq.logNumberNotification.set(idAssigner);

      idAssigner++;
    }
    return newLogEntries;
  }

  @FiberOnly
  private void leaderLogNewEntries(List<LogEntry> newLogEntries, long lastIndexInList) {
    final ListenableFuture<Boolean> localLogFuture;

    if (newLogEntries.isEmpty()) {
      return;
    }
    localLogFuture = log.logEntries(newLogEntries);

    Futures.addCallback(localLogFuture, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
        assert result != null && result;

        peersLastAckedIndex.put(myId, lastIndexInList);
        checkIfMajorityCanCommit(lastIndexInList);
      }

      @Override
      public void onFailure(Throwable t) {
        // pretty bad.
        logger.error("failed to commit to local log", t);
      }
    }, fiber);
  }

  @FiberOnly
  private void sendAppendEntries(long peer, long peerNextIdx, long lastIndexSent, final List<LogEntry> entries) {

    assert (entries.size() == 0) || (entries.get(0).getIndex() == peerNextIdx);
    assert (entries.size() == 0) || (entries.get(entries.size() - 1).getIndex() == lastIndexSent);

    final long prevLogIndex = peerNextIdx - 1;
    final long prevLogTerm;
    if (prevLogIndex == 0) {
      prevLogTerm = 0;
    } else {
      prevLogTerm = log.getLogTerm(prevLogIndex);
    }

    // catch them up so the next RPC wont over-send old junk.
    peersNextIndex.put(peer, lastIndexSent + 1);

    AppendEntries msg = new AppendEntries(
        currentTerm, myId, prevLogIndex, prevLogTerm,
        entries,
        lastCommittedIndex
    );

    RpcRequest request = new RpcRequest(peer, myId, quorumId, msg);
    AsyncRequest.withOneReply(fiber, sendRpcChannel, request, message -> {
      logger.trace("got a reply {}", message);

      boolean wasSuccessful = message.getAppendReplyMessage().getSuccess();
      if (!wasSuccessful) {
        // This is per Page 7, paragraph 5.  "After a rejection, the leader decrements nextIndex and retries"
        if (message.getAppendReplyMessage().getMyLastLogEntry() != 0) {
          peersNextIndex.put(peer, message.getAppendReplyMessage().getMyLastLogEntry());
        } else {
          peersNextIndex.put(peer, peerNextIdx - 1);
        }
      } else {
        // we have been successfully acked up to this point.
        logger.trace("peer {} acked for {}", peer, lastIndexSent);
        peersLastAckedIndex.put(peer, lastIndexSent);

        checkIfMajorityCanCommit(lastIndexSent);
      }
    }, 5, TimeUnit.SECONDS, () ->
        // Do nothing -> let next timeout handle things.
        // This timeout exists just so that we can cancel and clean up stuff in jetlang.
        logger.trace("peer {} timed out", peer));
  }

  @FiberOnly
  private void checkIfMajorityCanCommit(long lastAckedIndex) {
    /**
     * Determine if a majority of the current peers have replicated entries up to some index,
     * and thus it is safe to commit up to that index (aka declare it visible); if so, actually
     * commit. The argument is the last-updated index, but the actual index found to be able to
     * be committed by a majority could be less than or equal to that index.
     */

    if (lastAckedIndex <= lastCommittedIndex) {
      return; // already committed this entry
    }

    if (myFirstIndexAsLeader == 0) {
      return; // cant declare new visible yet until we have a first index as the leader.
    }

    /**
     * Goal: find an N such that N > lastCommittedIndex, a majority of peersLastAckedIndex[peerId] >= N
     * (considering peers within the current peer configuration) and N >= myFirstIndexAsLeader. If found,
     * commit up to the greatest such N. (sec 5.3, sec 5.4).
     */

    final long newCommitIndex = currentConfig.calculateCommittedIndex(peersLastAckedIndex);

    if (newCommitIndex < myFirstIndexAsLeader) {
      logger.warn("Found newCommitIndex index {} but my first index as leader was {}, cant declare visible yet",
          newCommitIndex, myFirstIndexAsLeader);
      return;
    }

    if (newCommitIndex < lastCommittedIndex) {
      logger.warn("weird newCommitIndex {} is smaller than lastCommittedIndex {}",
          newCommitIndex, lastCommittedIndex);
      return;
    }
    if (newCommitIndex == lastCommittedIndex) {
      return;
    }
    setLastCommittedIndex(newCommitIndex);
    logger.trace("discovered new visible entry {}", lastCommittedIndex);

    // TODO take action and notify clients (pending new system frameworks)
  }

  private void setLastCommittedIndex(long newLastCommittedIndex) {
    if (newLastCommittedIndex < lastCommittedIndex) {
      logger.warn("New lastCommittedIndex {} is smaller than previous lastCommittedIndex {}",
          newLastCommittedIndex, lastCommittedIndex);
    } else if (newLastCommittedIndex > lastCommittedIndex) {
      lastCommittedIndex = newLastCommittedIndex;
      notifyLastCommitted();
    }
  }

  private void notifyLastCommitted() {
    commitNoticeChannel.publish(new IndexCommitNotice(this, lastCommittedIndex));
  }

  private void setVotedFor(long votedFor) {
    try {
      persister.writeCurrentTermAndVotedFor(quorumId, currentTerm, votedFor);
    } catch (IOException e) {
      failReplicatorInstance(e);
    }

    this.votedFor = votedFor;
  }

  private void setCurrentTerm(long newTerm) {
    try {
      persister.writeCurrentTermAndVotedFor(quorumId, newTerm, 0);
    } catch (IOException e) {
      failReplicatorInstance(e);
    }

    this.currentTerm = newTerm;
    this.votedFor = 0;
  }

  private Logger getNewLogger() {
    return LoggerFactory.getLogger("(" + getClass().getSimpleName() + " - " + quorumId + " - " + myId + ")");
  }

  private int numPeers() {
    return currentConfig.allPeers().size();
  }

  private Set<Long> allPeersExceptMe() {
    return Sets.difference(currentConfig.allPeers(), Sets.newHashSet(myId));
  }

}
