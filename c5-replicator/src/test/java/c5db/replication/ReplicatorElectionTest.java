/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.replication;

import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.PreElectionPoll;
import c5db.replication.generated.PreElectionReply;
import c5db.replication.rpc.RpcMessage;
import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import c5db.util.CheckedConsumer;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import io.protostuff.Message;
import org.hamcrest.Matcher;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static c5db.AsyncChannelAsserts.ChannelHistoryMonitor;
import static c5db.RpcMatchers.ReplyMatcher.aPreElectionReply;
import static c5db.RpcMatchers.RequestMatcher;
import static c5db.RpcMatchers.RequestMatcher.aPreElectionPoll;
import static c5db.RpcMatchers.RequestMatcher.aRequestVote;
import static c5db.interfaces.replication.Replicator.State.CANDIDATE;
import static c5db.interfaces.replication.Replicator.State.FOLLOWER;
import static c5db.interfaces.replication.ReplicatorInstanceEvent.EventType.ELECTION_STARTED;
import static c5db.interfaces.replication.ReplicatorInstanceEvent.EventType.ELECTION_TIMEOUT;
import static c5db.replication.ReplicationMatchers.aReplicatorEvent;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ReplicatorElectionTest {
  private static final long CURRENT_TERM = 4;
  private static final String QUORUM_ID = "quorumId";
  private static final long MY_ID = 1;
  private static final int ELECTION_TIMEOUT_MILLIS = 25;

  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  @Rule
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();
  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);
  private final Fiber rpcFiber = new ThreadFiber(new RunnableExecutorImpl(batchExecutor), "rpcFiber-Thread", true);

  private final Channel<IndexCommitNotice> commitNotices = new MemoryChannel<>();
  private final RequestChannel<RpcRequest, RpcWireReply> sendRpcChannel = new MemoryRequestChannel<>();
  private final MemoryChannel<Request<RpcRequest, RpcWireReply>> requestChannel = new MemoryChannel<>();
  private final ChannelHistoryMonitor<Request<RpcRequest, RpcWireReply>> requestMonitor =
      new ChannelHistoryMonitor<>(requestChannel, rpcFiber);

  private final MemoryChannel<RpcWireReply> selfReplyChannel = new MemoryChannel<>();
  private final ChannelHistoryMonitor<RpcWireReply> selfReplyMonitor =
      new ChannelHistoryMonitor<>(selfReplyChannel, rpcFiber);

  private final MemoryChannel<ReplicatorInstanceEvent> eventChannel = new MemoryChannel<>();
  private final ChannelHistoryMonitor<ReplicatorInstanceEvent> eventMonitor =
      new ChannelHistoryMonitor<>(eventChannel, rpcFiber);

  private final ReplicatorInfoPersistence persistence = context.mock(ReplicatorInfoPersistence.class);
  private final ReplicatorLog log = context.mock(ReplicatorLog.class);
  private final InRamSim.StoppableClock clock = new InRamSim.StoppableClock(0, ELECTION_TIMEOUT_MILLIS);

  private ReplicatorInstance replicatorInstance;

  @Before
  public final void setupFibersAndChannels() throws Exception {
    context.checking(new Expectations() {{
      oneOf(persistence).readCurrentTerm(QUORUM_ID);
      will(returnValue(CURRENT_TERM));

      oneOf(persistence).readVotedFor(QUORUM_ID);
    }});

    sendRpcChannel.subscribe(rpcFiber, requestChannel::publish);

    rpcFiber.start();
  }

  @After
  public final void disposeResources() {
    if (replicatorInstance != null) {
      replicatorInstance.dispose();
    }

    rpcFiber.dispose();
  }

  private void withLogReflectingConfiguration(QuorumConfiguration configuration) {
    context.checking(new Expectations() {{
      allowing(log).getLastConfiguration();
      will(returnValue(configuration));

      allowing(log).getLastConfigurationIndex();
      will(returnValue(1L));

      allowing(log).getLastIndex();
      will(returnValue(1L));

      allowing(log).getLastTerm();
      will(returnValue(CURRENT_TERM));

      allowing(log).getLogTerm(1L);
      will(returnValue(CURRENT_TERM));
    }});
  }

  @Test
  public void ifAFollowerTimesOutItWillSendAPreElectionRequestToAllPeersIncludingItself() throws Exception {
    withLogReflectingConfiguration(aFiveNodeConfiguration());
    whenTheReplicatorIsInState(FOLLOWER);

    allowReplicatorToTimeout();

    expectReplicatorToEmitEvent(aReplicatorEvent(ELECTION_TIMEOUT));
    allPeers((peerId) ->
        expectReplicatorToSend(aPreElectionPoll().to(peerId)));
  }

  @Test
  public void ifAFollowerWhoTimedOutReceivesPreElectionRepliesFromAMajorityOfServersThenItWillInitiateAnElection()
      throws Exception {
    final QuorumConfiguration configuration = aFiveNodeConfiguration();
    withLogReflectingConfiguration(configuration);
    context.checking(expectTermIncrementAndVote());

    whenTheReplicatorIsInState(FOLLOWER);

    havingTimedOutAndInitiatedAPreElectionPoll();

    expectReplicatorToReplyToItself(aPreElectionReply().withPollResult(true));

    chooseTwo(otherPeers(configuration)).forEach((peerId) ->
        withTheRequest(aPreElectionPoll().to(peerId)).receiveReply(preElectionReply(true)));

    expectReplicatorToEmitEvent(aReplicatorEvent(ELECTION_STARTED));
    allPeers((peerId) ->
        expectReplicatorToSend(aRequestVote().to(peerId)));
  }

  @Test
  public void ifAFollowerReceivesAnRpcDuringAPreElectionPollThenItWillNotInitiateAnElectionEvenIfItGetsEnoughVotes()
      throws Exception {
    final QuorumConfiguration configuration = aFiveNodeConfiguration();
    withLogReflectingConfiguration(configuration);
    whenTheReplicatorIsInState(FOLLOWER);

    havingTimedOutAndInitiatedAPreElectionPoll();

    allowTimeToPass();
    receiveAnAppendMessage();

    otherPeers(configuration).forEach((peerId) ->
        withTheRequest(aPreElectionPoll().to(peerId)).receiveReply(preElectionReply(true)));

    // To observe that the replicator will *not* start an election, wait for some other event to occur
    havingTimedOutAndInitiatedAPreElectionPoll();

    assertThat(theReplicatorHasStartedAnElection(), is(false));
  }

  @Test
  public void ifAFollowerTimesOutBeforeAPollCompletesThenTheFollowerWillBeginAnotherPollAndIgnoreTheResultsOfTheFirst()
      throws Exception {
    final QuorumConfiguration configuration = aFiveNodeConfiguration();
    withLogReflectingConfiguration(configuration);
    whenTheReplicatorIsInState(FOLLOWER);

    havingTimedOutAndInitiatedAPreElectionPoll();

    Set<SentRequest> firstPollRequests = captureRequestsTo(otherPeers(configuration), aPreElectionPoll());

    allowReplicatorToTimeout();
    expectReplicatorToEmitEvent(aReplicatorEvent(ELECTION_TIMEOUT));

    firstPollRequests.forEach((request) -> request.receiveReply(preElectionReply(true)));

    // To observe that the replicator will *not* start an election, wait for some other event to occur
    havingTimedOutAndInitiatedAPreElectionPoll();

    assertThat(theReplicatorHasStartedAnElection(), is(false));
  }

  @Test
  public void ifACandidateTimesOutItWillIncrementItsTermAndBeginAnotherElection() throws Exception {
    withLogReflectingConfiguration(aFiveNodeConfiguration());
    context.checking(expectTermIncrementAndVote());

    whenTheReplicatorIsInState(CANDIDATE);

    allowReplicatorToTimeout();

    expectReplicatorToEmitEvent(aReplicatorEvent(ELECTION_STARTED));
    allPeers((peerId) ->
        expectReplicatorToSend(aRequestVote().to(peerId)));
  }

  @Test
  public void receivingAPreElectionPollRequestDoesNotCountAsAnRpcForTheSakeOfPreventingNodesFromTimingOut()
      throws Exception {
    final QuorumConfiguration configuration = aFiveNodeConfiguration();
    withLogReflectingConfiguration(configuration);
    whenTheReplicatorIsInState(FOLLOWER);

    receiveAnAppendMessage();

    clock.advanceTime(clock.electionTimeout() - 1);

    havingReceived(
        aPreElectionPollRequestWithSameLastIndexAndTermFrom(chooseOne(otherPeers(configuration))),
        (ignoreReply) -> {
        });

    clock.advanceTime(2);
    expectReplicatorToEmitEvent(aReplicatorEvent(ELECTION_TIMEOUT));
  }

  @Test
  public void aFollowerWillReplyFalseToAPreElectionPollFromAPeerInTheRemovedSetDuringATransitionalConfiguration()
      throws Exception {
    final QuorumConfiguration configuration = aTransitionalConfigurationWithThisPeerInTheNewSet();
    final long peerBeingRemoved = chooseOne(otherPeersBeingRemoved(configuration));
    final long peerNotBeingRemoved = chooseOne(otherPeersNotBeingRemoved(configuration));

    withLogReflectingConfiguration(configuration);
    whenTheReplicatorIsInState(FOLLOWER);

    havingReceived(
        aPreElectionPollRequestWithSameLastIndexAndTermFrom(peerBeingRemoved),
        (reply) ->
            assertThat(reply, is(aPreElectionReply().withPollResult(false))));

    havingReceived(
        aPreElectionPollRequestWithSameLastIndexAndTermFrom(peerNotBeingRemoved),
        (reply) ->
            assertThat(reply, is(aPreElectionReply().withPollResult(true))));
  }

  @Test
  public void aCandidateMayReplyTrueToAPreElectionPollFromAPeerInTheRemovedSetDuringATransitionalConfiguration()
      throws Exception {

    final QuorumConfiguration transitionalConfiguration = aTransitionalConfigurationWithThisPeerInTheNewSet();
    final long aPeerBeingRemoved = chooseOne(otherPeersBeingRemoved(transitionalConfiguration));

    withLogReflectingConfiguration(transitionalConfiguration);
    whenTheReplicatorIsInState(CANDIDATE);

    havingReceived(
        aPreElectionPollRequestWithSameLastIndexAndTermFrom(aPeerBeingRemoved),
        (reply) ->
            assertThat(reply, is(aPreElectionReply().withPollResult(true))));
  }


  private void whenTheReplicatorIsInState(Replicator.State state) throws Exception {
    final Fiber replicatorFiber = new ThreadFiber(new RunnableExecutorImpl(batchExecutor),
        "replicatorFiber-Thread", true);

    replicatorInstance = new ReplicatorInstance(replicatorFiber,
        MY_ID,
        QUORUM_ID,
        log,
        clock,
        persistence,
        sendRpcChannel,
        eventChannel,
        commitNotices,
        state);

    sendRpcChannel.subscribe(rpcFiber, (request) -> {
      if (request.getRequest().to == MY_ID) {
        handleLoopBack(request);
      }
    });

    replicatorInstance.start();
  }

  private SentRequest withTheRequest(RequestMatcher requestMatcher) {
    return new SentRequest(requestMonitor.waitFor(requestMatcher));
  }

  private PreElectionReply preElectionReply(boolean wouldVote) {
    return new PreElectionReply(CURRENT_TERM, wouldVote);
  }

  private RpcWireRequest appendEntriesRequest() {
    long from = chooseOne(log.getLastConfiguration().allPeers());
    return new RpcWireRequest(from, QUORUM_ID,
        new AppendEntries(CURRENT_TERM, from, log.getLastIndex(), log.getLastTerm(), new ArrayList<>(),
            lastCommittedIndex(0)));
  }

  private void allowTimeToPass() throws Exception {
    clock.advanceTime(5);
  }

  private void receiveAnAppendMessage() throws Exception {
    havingReceived(appendEntriesRequest(), (ignoreReply) -> {
    });
  }

  private void havingTimedOutAndInitiatedAPreElectionPoll() throws Exception {
    allowReplicatorToTimeout();

    expectReplicatorToEmitEvent(aReplicatorEvent(ELECTION_TIMEOUT));
    clock.stopTimeout();
    eventMonitor.forgetHistory();

    allPeers((peerId) ->
        expectReplicatorToSend(aPreElectionPoll().to(peerId)));
  }

  private void havingReceived(RpcWireRequest request, Consumer<RpcReply> onReply)
      throws Exception {
    SettableFuture<Boolean> finished = SettableFuture.create();
    AsyncRequest.withOneReply(rpcFiber, replicatorInstance.getIncomingChannel(), request,
        (reply) -> {
          onReply.accept(reply);
          finished.set(true);
        });
    finished.get(4, TimeUnit.SECONDS);
  }

  private Set<SentRequest> captureRequestsTo(Set<Long> peerIds, RequestMatcher requestMatcher) {
    return peerIds.stream()
        .map((peerId) -> withTheRequest(requestMatcher.to(peerId)))
        .collect(Collectors.toSet());
  }

  private void allPeers(CheckedConsumer<Long, Exception> forEach) throws Exception {
    for (long peerId : log.getLastConfiguration().allPeers()) {
      forEach.accept(peerId);
    }
  }

  private void expectReplicatorToSend(RequestMatcher requestMatcher) {
    requestMonitor.waitFor(requestMatcher.from(MY_ID));
  }

  private void expectReplicatorToReplyToItself(Matcher<RpcMessage> replyMatcher) {
    selfReplyMonitor.waitFor(replyMatcher);
  }

  private void expectReplicatorToEmitEvent(Matcher<ReplicatorInstanceEvent> eventMatcher) {
    eventMonitor.waitFor(eventMatcher);
  }

  private Expectations expectTermIncrementAndVote() throws Exception {
    return new Expectations() {{
      oneOf(persistence).writeCurrentTermAndVotedFor(
          with(equalTo(QUORUM_ID)), with(equalTo(CURRENT_TERM + 1)), with(anyVotedFor()));

      // May vote for self
      allowing(persistence).writeCurrentTermAndVotedFor(QUORUM_ID, CURRENT_TERM + 1, MY_ID);
    }};
  }

  private boolean theReplicatorHasStartedAnElection() {
    return eventMonitor.hasAny(aReplicatorEvent(ELECTION_STARTED));
  }

  private void allowReplicatorToTimeout() {
    clock.startTimeout();
  }

  private void handleLoopBack(Request<RpcRequest, RpcWireReply> request) {
    final RpcWireRequest newRequest = new RpcWireRequest(MY_ID, QUORUM_ID, request.getRequest().message);

    AsyncRequest.withOneReply(rpcFiber, replicatorInstance.getIncomingChannel(), newRequest,
        reply -> {
          final RpcWireReply wireReply = new RpcWireReply(MY_ID, MY_ID, QUORUM_ID, reply.message);
          selfReplyChannel.publish(wireReply);
          request.reply(wireReply);
        });
  }

  private static class SentRequest {
    public final Request<RpcRequest, RpcWireReply> request;

    private SentRequest(Request<RpcRequest, RpcWireReply> request) {
      this.request = request;
    }

    public void receiveReply(Message message) {
      RpcRequest requestMessage = request.getRequest();
      request.reply(
          new RpcWireReply(requestMessage.from, requestMessage.to, QUORUM_ID, message));
    }
  }

  private RpcWireRequest aPreElectionPollRequestWithSameLastIndexAndTermFrom(long from) {
    return new RpcWireRequest(from, QUORUM_ID,
        new PreElectionPoll(CURRENT_TERM, from, log.getLastIndex(), log.getLastTerm()));
  }

  private long lastCommittedIndex(long index) {
    return index;
  }

  private static Matcher<Long> anyVotedFor() {
    return any(Long.class);
  }

  private Set<Long> otherPeers(QuorumConfiguration configuration) {
    return Sets.filter(configuration.allPeers(), (id) -> id != MY_ID);
  }

  private Set<Long> otherPeersBeingRemoved(QuorumConfiguration configuration) {
    assert configuration.isTransitional;
    return Sets.filter(Sets.difference(configuration.prevPeers(), configuration.nextPeers()), (id) -> id != MY_ID);
  }

  private Set<Long> otherPeersNotBeingRemoved(QuorumConfiguration configuration) {
    assert configuration.isTransitional;
    return Sets.filter(configuration.nextPeers(), (id) -> id != MY_ID);
  }

  private <T> T chooseOne(Collection<T> collection) {
    assert !collection.isEmpty();
    return collection.iterator().next();
  }

  private <T> Collection<T> chooseTwo(Collection<T> collection) {
    assert collection.size() >= 2;
    return Sets.newHashSet(Iterables.limit(collection, 2));
  }

  private QuorumConfiguration aFiveNodeConfiguration() {
    return QuorumConfiguration.of(Lists.newArrayList(1L, 2L, 3L, 4L, 5L));
  }

  private QuorumConfiguration aTransitionalConfigurationWithThisPeerInTheNewSet() {
    return QuorumConfiguration.of(Lists.newArrayList(-101L, -102L, -103L))
        .getTransitionalConfiguration(Lists.newArrayList(1L, 2L, 3L, 4L, 5L));
  }
}
