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

import c5db.ReplicatorConstants;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.interfaces.replication.ReplicatorReceipt;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.AppendEntriesReply;
import c5db.replication.generated.LogEntry;
import c5db.replication.generated.PreElectionPoll;
import c5db.replication.generated.PreElectionReply;
import c5db.replication.generated.RequestVote;
import c5db.replication.generated.RequestVoteReply;
import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import c5db.util.C5Futures;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.Nullable;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.ChannelSubscription;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static c5db.ReplicatorConstants.REPLICATOR_APPEND_RPC_TIMEOUT_MILLISECONDS;
import static c5db.ReplicatorConstants.REPLICATOR_VOTE_RPC_TIMEOUT_MILLISECONDS;


/**
 * Single instantiation of a replicator / log / lease. This implementation's logic is based on the
 * RAFT algorithm (see <a href="http://raftconsensus.github.io/">http://raftconsensus.github.io/</a>.
 * <p>
 * A ReplicatorInstance handles the consensus and replication for a single quorum, and communicates
 * with the log package via {@link c5db.interfaces.replication.ReplicatorLog}.
 */
public class ReplicatorInstance implements Replicator {
  private final Channel<State> stateMemoryChannel = new MemoryChannel<>();
  private final RequestChannel<RpcWireRequest, RpcReply> incomingChannel = new MemoryRequestChannel<>();
  private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel;
  private final Channel<ReplicatorInstanceEvent> eventChannel;
  private final Channel<IndexCommitNotice> commitNoticeChannel;

  private final Fiber fiber;
  private final long myId;
  private final String quorumId;
  private final Logger logger;
  private final ReplicatorLog log;
  private final long myElectionTimeout;

  final ReplicatorClock clock;
  final ReplicatorInfoPersistence persister;

  /**
   * state used by leader
   */

  private final BlockingQueue<InternalReplicationRequest> logRequests =
      new ArrayBlockingQueue<>(ReplicatorConstants.REPLICATOR_MAXIMUM_SIMULTANEOUS_LOG_REQUESTS);

  // this is the next index from our log we need to send to each peer, kept track of on a per-peer basis.
  private final Map<Long, Long> peersNextIndex = new HashMap<>();

  // The last successfully acked message from our peers.  I also keep track of my own acked log messages in here.
  private final Map<Long, Long> peersLastAckedIndex = new HashMap<>();

  private long myFirstIndexAsLeader;
  private long lastCommittedIndex;

  /**
   * state, in theory persistent
   */

  long currentTerm;
  private long votedFor;
  private QuorumConfiguration quorumConfig = QuorumConfiguration.EMPTY;
  private long quorumConfigIndex = 0;

  /**
   * state, not persistent
   */

  State myState = State.FOLLOWER;

  // Election timers, etc.
  private long lastRPC;
  private long whosLeader = 0;
  @SuppressWarnings("UnusedDeclaration")
  private final Disposable electionChecker;


  public ReplicatorInstance(final Fiber fiber,
                            final long myId,
                            final String quorumId,
                            ReplicatorLog log,
                            ReplicatorClock clock,
                            ReplicatorInfoPersistence persister,
                            RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel,
                            final Channel<ReplicatorInstanceEvent> eventChannel,
                            final Channel<IndexCommitNotice> commitNoticeChannel,
                            State initialState) {
    this.fiber = fiber;
    this.myId = myId;
    this.quorumId = quorumId;
    this.logger = getNewLogger();
    this.sendRpcChannel = sendRpcChannel;
    this.log = log;
    this.clock = clock;
    this.persister = persister;
    this.eventChannel = eventChannel;
    this.commitNoticeChannel = commitNoticeChannel;
    this.myElectionTimeout = clock.electionTimeout();
    this.lastRPC = clock.currentTimeMillis();

    refreshQuorumConfigurationFromLog();

    commitNoticeChannel.subscribe(
        new ChannelSubscription<>(fiber, this::onCommit,
            (notice) ->
                notice.nodeId == myId
                    && notice.quorumId.equals(quorumId)));

    incomingChannel.subscribe(fiber, this::onIncomingMessage);
    electionChecker = fiber.scheduleWithFixedDelay(this::checkOnElection, clock.electionCheckInterval(),
        clock.electionCheckInterval(), TimeUnit.MILLISECONDS);

    this.myState = initialState;

    fiber.execute(() -> {
      try {
        readPersistentData();
        // indicate we are running!
        eventChannel.publish(
            new ReplicatorInstanceEvent(
                ReplicatorInstanceEvent.EventType.QUORUM_START,
                ReplicatorInstance.this,
                0,
                0,
                clock.currentTimeMillis(),
                null, null)
        );

        if (initialState == State.LEADER) {
          becomeLeader();
        }
      } catch (IOException e) {
        logger.error("error during persistent data init", e);
        failReplicatorInstance(e);
      }
    });
  }

  /**
   * public API
   */

  @Override
  public String getQuorumId() {
    return quorumId;
  }

  @Override
  public QuorumConfiguration getQuorumConfiguration() {
    return quorumConfig;
  }

  @Override
  public ListenableFuture<ReplicatorReceipt> changeQuorum(Collection<Long> newPeers) throws InterruptedException {
    if (!isLeader()) {
      logger.debug("attempted to changeQuorum on a non-leader");
      return null;
    }

    final QuorumConfiguration transitionConfig = quorumConfig.getTransitionalConfiguration(newPeers);
    return putQuorumChangeRequest(transitionConfig);
  }

  @Override
  public ListenableFuture<ReplicatorReceipt> logData(List<ByteBuffer> data) throws InterruptedException {
    if (!isLeader()) {
      logger.debug("attempted to logData on a non-leader");
      return null;
    }

    InternalReplicationRequest req = InternalReplicationRequest.toLogData(data);
    logRequests.put(req);

    // TODO return the durable notification future?
    return req.logReceiptFuture;
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
        ", quorumConfig=" + quorumConfig +
        '}';
  }

  @Override
  public Subscriber<State> getStateChannel() {
    return stateMemoryChannel;
  }

  @Override
  public Subscriber<ReplicatorInstanceEvent> getEventChannel() {
    return this.eventChannel;
  }

  @Override
  public Subscriber<IndexCommitNotice> getCommitNoticeChannel() {
    return commitNoticeChannel;
  }

  public RequestChannel<RpcWireRequest, RpcReply> getIncomingChannel() {
    return incomingChannel;
  }

  public void dispose() {
    fiber.dispose();
  }

  /**
   * Call this method on each replicator in a new quorum in order to establish the quorum
   * configuration and elect a leader.
   * <p>
   * Before a quorum (a group of cooperating replicators) may process replication requests
   * it must elect a leader. But a leader cannot be elected unless all peers are aware of
   * the members of the quorum, which in turn requires all to log a quorum configuration
   * entry.
   * <p>
   * This method logs that first entry. Unlike normal logging, it is not directed by a
   * leader; however, the logged entry cannot be confirmed until a leader is chosen. The
   * method should be called on every peer in the quorum being established. (Only a majority
   * need to successfully log the entry in order for the quorum establishment to go through).
   *
   * @param peerIds Collection of peers in the new quorum.
   * @return A future which will return when the peer has processed the bootstrap request.
   * The actual completion of the bootstrap will be signaled by a ReplicatorInstanceEvent
   * with the appropriate quorum configuration.
   */
  public ListenableFuture<Void> bootstrapQuorum(Collection<Long> peerIds) {
    assert peerIds.size() > 0;

    SettableFuture<Void> quorumPersistedFuture = SettableFuture.create();

    long seqNum = 1;
    QuorumConfiguration config = QuorumConfiguration.of(peerIds);
    LogEntry configEntry = InternalReplicationRequest.toChangeConfig(config).getEntry(0, seqNum);
    AppendEntries message = new AppendEntries(0, 0, 0, 0, Lists.newArrayList(configEntry), 0);
    RpcWireRequest request = new RpcWireRequest(myId, quorumId, message);

    // Send the append entries message to our own incoming message channel; we will receive it
    // and log the quorum configuration entry as usual.
    AsyncRequest.withOneReply(fiber, getIncomingChannel(), request, msg -> quorumPersistedFuture.set(null));

    return quorumPersistedFuture;
  }

  void failReplicatorInstance(Throwable e) {
    eventChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.QUORUM_FAILURE,
            this,
            0,
            0,
            clock.currentTimeMillis(),
            null,
            e)
    );
    fiber.dispose(); // kill us forever.
  }

  private void setState(State state) {
    myState = state;
    stateMemoryChannel.publish(state);
  }

  /**
   * Submits a request to change the quorum, blocking if necessary until the request
   * queue can accept the request.
   *
   * @return A future which will return the log index of the quorum configuration entry,
   */
  private ListenableFuture<ReplicatorReceipt> putQuorumChangeRequest(QuorumConfiguration quorumConfig)
      throws InterruptedException {
    InternalReplicationRequest req = InternalReplicationRequest.toChangeConfig(quorumConfig);
    logRequests.put(req);
    return req.logReceiptFuture;
  }

  /**
   * Submits a request to change the quorum, but only if it is possible to do so without blocking.
   *
   * @return A future which will return the log index of the quorum configuration entry,
   * or null if it was not possible to submit the request without blocking.
   */
  @FiberOnly
  @Nullable
  private ListenableFuture<ReplicatorReceipt> offerQuorumChangeRequest(QuorumConfiguration quorumConfig) {
    if (this.quorumConfig.equals(quorumConfig)) {
      logger.warn("got a request to change quorum to but I'm already in that quorum config {} ", quorumConfig);
      return null;
    }

    InternalReplicationRequest req = InternalReplicationRequest.toChangeConfig(quorumConfig);
    if (logRequests.offer(req)) {
      return req.logReceiptFuture;
    } else {
      logger.warn("change request could not be submitted because log request queue was full {}", quorumConfig);
      return null;
    }
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
      if (req.isPreElectionPollMessage()) {
        doPreElectionPollMessage(message);

      } else if (req.isRequestVoteMessage()) {
        doRequestVote(message);

      } else if (req.isAppendMessage()) {
        doAppendMessage(message);

      } else {
        logger.warn("got a message of protobuf type I don't know: {}", req);
      }
    } catch (Exception e) {
      logger.error("Uncaught exception while processing message {}: {}", message, e);
      throw e;
    }
  }

  @FiberOnly
  private void doPreElectionPollMessage(Request<RpcWireRequest, RpcReply> message) {
    final RpcWireRequest request = message.getRequest();
    final PreElectionPoll msg = request.getPreElectionPollMessage();
    final long msgLastLogTerm = msg.getLastLogTerm();
    final long msgLastLogIndex = msg.getLastLogIndex();

    final boolean wouldVote =
        msg.getTerm() >= currentTerm
            && atLeastAsUpToDateAsLocalLog(msgLastLogTerm, msgLastLogIndex)
            && !rejectPollFromOldConfiguration(request.from, msgLastLogTerm, msgLastLogIndex);

    logger.debug("sending pre-election reply to {} wouldVote = {}", message.getRequest().from, wouldVote);
    PreElectionReply m = new PreElectionReply(currentTerm, wouldVote);
    RpcReply reply = new RpcReply(m);
    message.reply(reply);
  }

  /**
   * Special case for pre-election poll. If the sender's log exactly matches our log, but they
   * are in the process of being phased out in a transitional configuration (i.e., not a part
   * of the next set of peers); and if we are in the FOLLOWER state and we are continuing into
   * the next configuration, then reply false to their poll request. Thus, in a transitional
   * configuration, a peer being phased out will not be able to initiate an election unless
   * a node *not* being phased out is already conducting an election; or, unless its log is
   * more advanced, in which case there may be a legitimate need for the phasing-out peer to
   * start an election.
   *
   * @param from            id of the peer who sent us a pre-election poll request
   * @param msgLastLogTerm  the term of the last entry in that peer's log
   * @param msgLastLogIndex the index of the last entry in that peer's log
   * @return true if we should reply false to the pre-election poll request on the basis described.
   */
  private boolean rejectPollFromOldConfiguration(long from, long msgLastLogTerm, long msgLastLogIndex) {
    return quorumConfig.isTransitional
        && msgLastLogTerm == log.getLastTerm()
        && msgLastLogIndex == log.getLastIndex()
        && !quorumConfig.nextPeers().contains(from)
        && quorumConfig.nextPeers().contains(myId)
        && myState == State.FOLLOWER;
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
    if (atLeastAsUpToDateAsLocalLog(msg.getLastLogTerm(), msg.getLastLogIndex())) {
      // we can vote for this because the candidate's log is at least as
      // complete as the local log.

      if (votedFor == 0 || votedFor == message.getRequest().from) {
        setVotedFor(message.getRequest().from);
        lastRPC = clock.currentTimeMillis();
        vote = true;
      }
    }

    logger.debug("sending vote reply to {} vote = {}, voted = {}", message.getRequest().from, votedFor, vote);
    RequestVoteReply m = new RequestVoteReply(currentTerm, vote);
    RpcReply reply = new RpcReply(m);
    message.reply(reply);
  }

  private boolean atLeastAsUpToDateAsLocalLog(long msgLastLogTerm, long msgLastLogIndex) {
    final long localLastLogTerm = log.getLastTerm();
    final long localLastLogIndex = log.getLastIndex();

    return msgLastLogTerm > localLastLogTerm
        || (msgLastLogTerm == localLastLogTerm && msgLastLogIndex >= localLastLogIndex);
  }

  @FiberOnly
  private void doAppendMessage(final Request<RpcWireRequest, RpcReply> request) {
    final AppendEntries appendMessage = request.getRequest().getAppendMessage();

    // 1. return if term < currentTerm (sec 5.1)
    if (appendMessage.getTerm() < currentTerm) {
      appendReply(request, false);
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
    lastRPC = clock.currentTimeMillis();

    long theLeader = appendMessage.getLeaderId();
    if (whosLeader != theLeader) {
      updateFollowersKnowledgeOfCurrentLeader(theLeader);
    }

    // 5. return failure if log doesn't contain an entry at
    // prevLogIndex whose term matches prevLogTerm (sec 5.3)
    // if msgPrevLogIndex == 0 -> special case of starting the log!
    long msgPrevLogIndex = appendMessage.getPrevLogIndex();
    long msgPrevLogTerm = appendMessage.getPrevLogTerm();
    if (msgPrevLogIndex != 0 && log.getLogTerm(msgPrevLogIndex) != msgPrevLogTerm) {
      AppendEntriesReply m = new AppendEntriesReply(currentTerm, false, log.getLastIndex() + 1);
      RpcReply reply = new RpcReply(m);
      request.reply(reply);
      return;
    }

    if (appendMessage.getEntriesList().isEmpty()) {
      appendReply(request, true);
      long newCommitIndex = Math.min(appendMessage.getCommitIndex(), log.getLastIndex());
      setLastCommittedIndex(newCommitIndex);
      return;
    }

    // 6. if existing entries conflict with new entries, delete all
    // existing entries starting with first conflicting entry (sec 5.3)
    // nb: The process in which we fix the local log may involve several async log operations, so that is entirely
    // hidden up in these futures.  Note that the process can fail, so we handle that as well.
    List<ListenableFuture<Boolean>> logOperationFutures = reconcileAppendMessageWithLocalLog(appendMessage);
    ListenableFuture<List<Boolean>> bundledLogFuture = Futures.allAsList(logOperationFutures);

    // wait for the log to commit before returning message.  But do so async.
    C5Futures.addCallback(bundledLogFuture,
        (resultList) -> {
          appendReply(request, true);

          // 8. Signal the client of the Replicator that it can apply newly committed entries to state machine
          long newCommitIndex = Math.min(appendMessage.getCommitIndex(), log.getLastIndex());
          setLastCommittedIndex(newCommitIndex);
        },
        (Throwable t) -> {
          // TODO A log commit failure is probably a fatal error. Quit the instance?
          // TODO better error reporting. A log commit failure will be a serious issue.
          logger.error("failure reconciling received entries with the local log", t);
          appendReply(request, false);
        }, fiber);
  }

  private void appendReply(Request<RpcWireRequest, RpcReply> request, boolean success) {
    AppendEntriesReply m = new AppendEntriesReply(currentTerm, success, 0);
    RpcReply reply = new RpcReply(m);
    request.reply(reply);
  }

  @FiberOnly
  private void updateFollowersKnowledgeOfCurrentLeader(long theLeader) {
    logger.debug("discovered new leader: {}", theLeader);
    whosLeader = theLeader;

    eventChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.LEADER_ELECTED,
            this,
            whosLeader,
            currentTerm,
            clock.currentTimeMillis(),
            null, null)
    );
  }

  private List<ListenableFuture<Boolean>> reconcileAppendMessageWithLocalLog(AppendEntries appendMessage) {
    List<ListenableFuture<Boolean>> logOperationFutures = new ArrayList<>();

    // 6. if existing entries conflict with new entries, delete all
    // existing entries starting with first conflicting entry (sec 5.3)

    long nextIndex = log.getLastIndex() + 1;

    List<LogEntry> entriesFromMessage = appendMessage.getEntriesList();
    List<LogEntry> entriesToCommit = new ArrayList<>(entriesFromMessage.size());

    for (LogEntry entry : entriesFromMessage) {
      long entryIndex = entry.getIndex();

      if (entryIndex == nextIndex) {
        entriesToCommit.add(entry);
        nextIndex++;
        continue;
      }

      if (entryIndex > nextIndex) {
        // ok this entry is still beyond the LAST entry, so we have a problem:
        logger.warn("Received entry later in sequence than expected: expected {} but the next in the message is {}",
            nextIndex, entryIndex);
        logOperationFutures.add(Futures.immediateFailedFuture(
            new Exception("Unexpected log entry, or entry sequence gap in received entries")));
        return logOperationFutures;
      }

      // at this point entryIndex should be <= log.getLastIndex
      assert entryIndex < nextIndex;

      if (log.getLogTerm(entryIndex) != entry.getTerm()) {
        // This is generally expected to be fairly uncommon.
        // conflict:
        logger.debug("log conflict at idx {} my term: {} term from leader: {}, truncating log after this point",
            entryIndex, log.getLogTerm(entryIndex), entry.getTerm());

        // delete this and all subsequent entries from the local log.
        logOperationFutures.add(log.truncateLog(entryIndex));

        entriesToCommit.add(entry);
        nextIndex = entryIndex + 1;
      }
    }

    // 7. Append any new entries not already in the log.
    logOperationFutures.add(log.logEntries(entriesToCommit));
    refreshQuorumConfigurationFromLog();
    return logOperationFutures;
  }

  @FiberOnly
  private void checkOnElection() {
    if (myState == State.LEADER) {
      logger.trace("leader during election check.");
      return;
    }

    if (lastRPC + this.myElectionTimeout < clock.currentTimeMillis()
        && quorumConfig.allPeers().contains(myId)) {
      logger.trace("timed out checking on election, try new election");
      if (myState == State.CANDIDATE) {
        doElection();
      } else {
        doPreElection();
      }
    }
  }

  @FiberOnly
  private void doPreElection() {
    eventChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.ELECTION_TIMEOUT,
            this,
            0,
            0,
            clock.currentTimeMillis(),
            null, null)
    );

    // Start new election "timer".
    lastRPC = clock.currentTimeMillis();

    PreElectionPoll msg = new PreElectionPoll(currentTerm, myId, log.getLastIndex(), log.getLastTerm());

    logger.debug("starting pre-election poll for currentTerm: {}", currentTerm);

    final long termBeingVotedFor = currentTerm;
    final Set<Long> votes = new HashSet<>();

    for (long peer : quorumConfig.allPeers()) {
      final RpcRequest request = new RpcRequest(peer, myId, quorumId, msg);
      sendPreElectionRequest(request, termBeingVotedFor, votes, lastRPC);
    }
  }

  @FiberOnly
  private void sendPreElectionRequest(RpcRequest request, long termBeingVotedFor, Set<Long> votes, long startTime) {
    AsyncRequest.withOneReply(fiber, sendRpcChannel, request,
        message -> handlePreElectionReply(message, termBeingVotedFor, votes, startTime),
        REPLICATOR_VOTE_RPC_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS,
        () -> handlePreElectionTimeout(request, termBeingVotedFor, votes, startTime));
  }

  @FiberOnly
  private void handlePreElectionTimeout(RpcRequest request, long termBeingVotedFor, Set<Long> votes, long startTime) {
    if (myState != State.FOLLOWER
        || lastRPC != startTime) {
      return;
    }

    // Also if the term goes forward somehow, this is also out of date, and drop it.
    if (currentTerm > termBeingVotedFor) {
      logger.trace("pre-election poll timeout, current term has moved on, abandoning this request");
      return;
    }

    logger.trace("pre-election poll timeout to {}, resending RPC", request.to);
    sendPreElectionRequest(request, termBeingVotedFor, votes, startTime);
  }

  @FiberOnly
  private void handlePreElectionReply(RpcWireReply message, long termBeingVotedFor, Set<Long> votes, long startTime) {
    if (message == null) {
      logger.warn("got a NULL message reply, that's unfortunate");
      return;
    }

    if (currentTerm > termBeingVotedFor) {
      logger.warn("pre-election reply from {}, but currentTerm {} > vote term {}", message.from,
          currentTerm, termBeingVotedFor);
      return;
    }

    if (myState != State.FOLLOWER) {
      // we became not, ignore
      logger.warn("pre-election reply from {} ignored -> in state {}", message.from, myState);
      return;
    }

    if (lastRPC != startTime) {
      // Another timeout occurred, so this poll is moot.
      return;
    }

    PreElectionReply reply = message.getPreElectionReplyMessage();

    if (reply.getTerm() > currentTerm) {
      logger.warn("election reply from {}, but term {} was not my term {}, updating currentTerm",
          message.from, reply.getTerm(), currentTerm);

      setCurrentTerm(reply.getTerm());
      return;
    }

    if (reply.getWouldVote()) {
      votes.add(message.from);
    }

    if (votesConstituteMajority(votes)) {
      doElection();
    }
  }

  @FiberOnly
  private void doElection() {
    eventChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.ELECTION_STARTED,
            this,
            0,
            0,
            clock.currentTimeMillis(),
            null, null)
    );

    // Start new election "timer".
    lastRPC = clock.currentTimeMillis();
    // increment term.
    setCurrentTerm(currentTerm + 1);
    setState(State.CANDIDATE);

    RequestVote msg = new RequestVote(currentTerm, myId, log.getLastIndex(), log.getLastTerm());

    logger.debug("starting election for currentTerm: {}", currentTerm);

    final long termBeingVotedFor = currentTerm;
    final Set<Long> votes = new HashSet<>();
    for (long peer : quorumConfig.allPeers()) {
      RpcRequest req = new RpcRequest(peer, myId, quorumId, msg);
      AsyncRequest.withOneReply(fiber, sendRpcChannel, req,
          message -> handleElectionReply0(message, termBeingVotedFor, votes),
          REPLICATOR_VOTE_RPC_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS,
          new RequestVoteTimeout(req, termBeingVotedFor, votes));
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
          REPLICATOR_VOTE_RPC_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS, this);
    }
  }

  // RPC callback for timeouts
  @FiberOnly
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
    return quorumConfig.setContainsMajority(votes);
  }


  //// Leader timer stuff below
  private Disposable queueConsumer;

  @FiberOnly
  private void becomeFollower() {
    boolean wasLeader = myState == State.LEADER;
    setState(State.FOLLOWER);

    if (wasLeader) {
      eventChannel.publish(
          new ReplicatorInstanceEvent(
              ReplicatorInstanceEvent.EventType.LEADER_DEPOSED,
              this,
              0,
              0,
              clock.currentTimeMillis(),
              null, null));
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

  @FiberOnly
  private void becomeLeader() {
    logger.warn("I AM THE LEADER NOW, commence AppendEntries RPCs term = {}", currentTerm);

    setState(State.LEADER);

    // Page 7, para 5
    long myNextLog = log.getLastIndex() + 1;

    peersLastAckedIndex.clear();
    peersNextIndex.clear();

    for (long peer : allPeersExceptMe()) {
      peersNextIndex.put(peer, myNextLog);
    }

    // none so far!
    myFirstIndexAsLeader = 0;

    eventChannel.publish(
        new ReplicatorInstanceEvent(
            ReplicatorInstanceEvent.EventType.LEADER_ELECTED,
            this,
            myId,
            currentTerm,
            clock.currentTimeMillis(),
            null, null)
    );

    startQueueConsumer();
  }

  @FiberOnly
  private void startQueueConsumer() {
    queueConsumer = fiber.scheduleAtFixedRate(() -> {
      try {
        consumeQueue();
        checkOnQuorumChange();
      } catch (Throwable t) {
        logger.error("Exception in consumeQueue: ", t);
        failReplicatorInstance(t);
      }
    }, 0, clock.leaderLogRequestsProcessingInterval(), TimeUnit.MILLISECONDS);
  }

  @FiberOnly
  private void consumeQueue() {
    // retrieve as many items as possible. send rpc.
    // If there are no pending log requests, this method sends out heartbeats.
    final List<InternalReplicationRequest> reqs = new ArrayList<>();

    logger.trace("queue consuming");
    while (logRequests.peek() != null) {
      reqs.add(logRequests.poll());
    }

    logger.trace("{} queue items to commit", reqs.size());

    final long firstIndexInList = log.getLastIndex() + 1;
    final long lastIndexInList = firstIndexInList + reqs.size() - 1;

    List<LogEntry> newLogEntries = createLogEntriesFromIntRequests(reqs, firstIndexInList);
    leaderLogNewEntries(newLogEntries, lastIndexInList);
    refreshQuorumConfigurationFromLog();

    assert lastIndexInList == log.getLastIndex();

    for (final long peer : allPeersExceptMe()) {

      // for each peer, figure out how many "back messages" should I send:
      final long peerNextIdx = this.peersNextIndex.getOrDefault(peer, firstIndexInList);

      if (peerNextIdx < firstIndexInList) {
        final long moreCount = firstIndexInList - peerNextIdx;
        logger.debug("sending {} more log entries to peer {}", moreCount, peer);

        // TODO check moreCount is reasonable, and available in log. Otherwise do alternative peer catch up
        // TODO alternative peer catchup is by a different process, send message to that then skip sending AppendRpc

        // TODO allow for smaller 'catch up' messages so we don't try to create a 400GB sized message.

        // TODO cache these extra LogEntry objects so we don't recreate too many of them.

        ListenableFuture<List<LogEntry>> peerEntriesFuture = log.getLogEntries(peerNextIdx, firstIndexInList);

        C5Futures.addCallback(peerEntriesFuture,
            (entriesFromLog) -> {
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
            },
            (Throwable t) -> {
              // TODO is this situation ever recoverable?
              logger.error("failed to retrieve from log", t);
              failReplicatorInstance(t);
            }, fiber);
      } else {
        sendAppendEntries(peer, peerNextIdx, lastIndexInList, newLogEntries);
      }
    }
  }

  @FiberOnly
  private void checkOnQuorumChange() {

    if (lastCommittedIndex >= quorumConfigIndex) {
      if (quorumConfig.isTransitional) {
        // Current transitional quorum is committed, so begin replicating the next stage.
        // If this call happens to fail because the log request queue is full, we'll catch
        // it the next time around.
        offerQuorumChangeRequest(quorumConfig.getCompletedConfiguration());
      } else {

        if (!quorumConfig.allPeers().contains(myId)) {
          // Committed, stable configuration
          // Resign if it does not include me.
          // TODO should there be a special event for this? Should the replicator shut itself down completely?
          becomeFollower();
        }
      }
    }
  }

  @FiberOnly
  private List<LogEntry> createLogEntriesFromIntRequests(List<InternalReplicationRequest> requests, long firstEntryIndex) {
    long idAssigner = firstEntryIndex;

    // Build the log entries:
    List<LogEntry> newLogEntries = new ArrayList<>(requests.size());
    for (InternalReplicationRequest logReq : requests) {
      final LogEntry entry = logReq.getEntry(currentTerm, idAssigner);
      newLogEntries.add(entry);

      if (myFirstIndexAsLeader == 0) {
        myFirstIndexAsLeader = idAssigner;
        logger.debug("my first index as leader is: {}", myFirstIndexAsLeader);
      }

      // let the client know what our id is
      logReq.logReceiptFuture.set(new ReplicatorReceipt(currentTerm, idAssigner));

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

    // TODO this callback and some others should have timeouts in case the log hangs somehow
    C5Futures.addCallback(localLogFuture,
        (result) -> {
          peersLastAckedIndex.put(myId, lastIndexInList);
          checkIfMajorityCanCommit(lastIndexInList);
        },
        (Throwable t) -> {
          logger.error("As a leader, failed to write new entries to local log", t);
          failReplicatorInstance(t);
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
        if (message.getAppendReplyMessage().getMyNextLogEntry() != 0) {
          peersNextIndex.put(peer, message.getAppendReplyMessage().getMyNextLogEntry());
        } else {
          peersNextIndex.put(peer, Math.max(peerNextIdx - 1, 1));
        }
      } else {
        // we have been successfully acked up to this point.
        logger.trace("peer {} acked for {}", peer, lastIndexSent);
        peersLastAckedIndex.put(peer, lastIndexSent);

        checkIfMajorityCanCommit(lastIndexSent);
      }
    }, REPLICATOR_APPEND_RPC_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS, () ->
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
      return; // can't declare new visible yet until we have a first index as the leader.
    }

    if (quorumConfig.isEmpty()) {
      return; // there are no peers in the quorum, so nothing can be committed
    }

    /**
     * Goal: find an N such that N > lastCommittedIndex, a majority of peersLastAckedIndex[peerId] >= N
     * (considering peers within the current peer configuration) and N >= myFirstIndexAsLeader. If found,
     * commit up to the greatest such N. (sec 5.3, sec 5.4).
     */

    final long newCommitIndex = quorumConfig.calculateCommittedIndex(peersLastAckedIndex);

    if (newCommitIndex <= lastCommittedIndex) {
      return;
    }

    if (newCommitIndex < myFirstIndexAsLeader) {
      logger.info("Found newCommitIndex {} but my first index as leader was {}, can't declare visible yet",
          newCommitIndex, myFirstIndexAsLeader);
      return;
    }

    if (newCommitIndex < lastCommittedIndex) {
      logger.warn("weird newCommitIndex {} is smaller than lastCommittedIndex {}",
          newCommitIndex, lastCommittedIndex);
      return;
    }

    setLastCommittedIndex(newCommitIndex);
    logger.trace("discovered new visible entry {}", lastCommittedIndex);
  }

  private void setLastCommittedIndex(long newLastCommittedIndex) {
    if (newLastCommittedIndex < lastCommittedIndex) {
      logger.info("Requested to update to commit index {} but it is smaller than my current commit index {}; ignoring",
          newLastCommittedIndex, lastCommittedIndex);
    } else if (newLastCommittedIndex > lastCommittedIndex) {
      long oldLastCommittedIndex = lastCommittedIndex;
      lastCommittedIndex = newLastCommittedIndex;
      issueCommitNotifications(oldLastCommittedIndex);
    }
  }

  private void issueCommitNotifications(long oldLastCommittedIndex) {
    // TODO inefficient, because it calls getLogTerm once for every index. Possible optimization here.
    final long firstCommittedIndex = oldLastCommittedIndex + 1;
    long firstIndexOfTerm = firstCommittedIndex;
    long nextTerm = log.getLogTerm(firstCommittedIndex);

    for (long index = firstCommittedIndex; index <= lastCommittedIndex; index++) {
      long currentTerm = nextTerm;
      if (index == lastCommittedIndex
          || (nextTerm = log.getLogTerm(index + 1)) != currentTerm) {
        commitNoticeChannel.publish(new IndexCommitNotice(quorumId, myId, firstIndexOfTerm, index, currentTerm));
        firstIndexOfTerm = index + 1;
      }
    }
  }

  private void onCommit(IndexCommitNotice notice) {
    if (notice.firstIndex <= quorumConfigIndex
        && quorumConfigIndex <= notice.lastIndex) {
      eventChannel.publish(
          new ReplicatorInstanceEvent(
              ReplicatorInstanceEvent.EventType.QUORUM_CONFIGURATION_COMMITTED,
              this,
              0,
              0,
              clock.currentTimeMillis(),
              quorumConfig,
              null));
    }
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

  private Set<Long> allPeersExceptMe() {
    return Sets.difference(quorumConfig.allPeers(), Sets.newHashSet(myId));
  }

  private void refreshQuorumConfigurationFromLog() {
    quorumConfig = log.getLastConfiguration();
    quorumConfigIndex = log.getLastConfigurationIndex();
  }
}
