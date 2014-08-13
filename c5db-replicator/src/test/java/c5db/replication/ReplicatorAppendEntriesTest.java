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
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.log.InRamLog;
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.LogEntry;
import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcWireRequest;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static c5db.AsyncChannelAsserts.ChannelHistoryMonitor;
import static c5db.FutureMatchers.resultsIn;
import static c5db.IndexCommitMatcher.aCommitNotice;
import static c5db.RpcMatchers.ReplyMatcher.anAppendReply;
import static c5db.interfaces.replication.Replicator.State;
import static c5db.log.ReplicatorLogGenericTestUtil.aSeqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.someData;
import static c5db.replication.ReplicationMatchers.aListOfEntriesWithConsecutiveSeqNums;
import static c5db.replication.ReplicationMatchers.leaderElectedEvent;
import static c5db.replication.ReplicatorTestUtil.LogSequenceBuilder;
import static c5db.replication.ReplicatorTestUtil.entries;
import static c5db.replication.ReplicatorTestUtil.makeProtostuffEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

/**
 * A class for testing a single ReplicatorInstance node, to determine if it reacts correctly to AppendEntries
 * messages.
 */
public class ReplicatorAppendEntriesTest {
  private ReplicatorInstance replicatorInstance;

  private static final long LEADER_ID = 2;
  private static final long CURRENT_TERM = 4;
  private static final String QUORUM_ID = "ReplicatorAppendEntriesTest-quorumId";
  private static final int RPC_REPLY_TIMEOUT = 2; // seconds

  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};
  private final ReplicatorInfoPersistence persistence = context.mock(ReplicatorInfoPersistence.class);

  // Use this field to express expectations on the replicator's log
  private final ReplicatorLog log = context.mock(ReplicatorLog.class);

  @Rule
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();
  private final BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(fiberExceptionHandler);

  private final Fiber rpcFiber = new ThreadFiber(new RunnableExecutorImpl(batchExecutor), null, true);

  @Before
  public void setOverallTestExpectations() throws Exception {
    context.checking(new Expectations() {{
      allowing(persistence).readCurrentTerm(QUORUM_ID);
      will(returnValue(CURRENT_TERM));

      allowing(persistence).readVotedFor(QUORUM_ID);
      will(returnValue(LEADER_ID));

      /* Place no constraint on the replicator's usage of these synchronous getters.
       * The replicator uses a Proxy ReplicatorLog which allows us to use jmock
       * expectations for the log, but also use a working InRamLog.
       */
      allowing(log).getLastIndex();
      allowing(log).getLastTerm();
      allowing(log).getLogTerm(with(any(Long.class)));
      allowing(log).getLastConfiguration();
      allowing(log).getLastConfigurationIndex();
    }});
  }

  @Before
  public void createAndStartReplicatorAndRpcFiber() throws Exception {
    replicatorInstance = makeTestInstance();
    replicatorInstance.start();
    rpcFiber.start();
  }

  @After
  public void disposeReplicatorAndRpcFiber() {
    replicatorInstance.dispose();
    rpcFiber.dispose();
  }

  @Test
  public void repliesFalseIfItReceivesAnEmptyAppendEntriesRequestWithAnOldTerm() throws Exception {
    havingReceived(
        anAppendEntriesRequest()
            .withAnOldTerm()
            .withNoEntries());

    assertThat(reply(), is(anAppendReply().withResult(false)));
  }

  @Test
  public void repliesFalseIfItReceivesANonemptyAppendEntriesRequestWithAnOldTerm() throws Exception {
    havingReceived(
        anAppendEntriesRequest()
            .withAnOldTerm()
            .withEntry(aLogEntry()));

    assertThat(reply(), is(anAppendReply().withResult(false)));
  }

  @Test
  public void repliesFalseIfItConflictsWithTheRequestOnTheTermOfTheLogEntryAtPrevLogIndex() throws Exception {
    final long termInLog = 3;
    final long termInMessage = 4;

    havingLogged(
        entries().term(termInLog).indexes(1, 2, 3));

    havingReceived(
        anAppendEntriesRequest()
            .withPrevLogTerm(termInMessage).withPrevLogIndex(1)
            .withEntries(entries().term(termInMessage).indexes(2, 3)));

    assertThat(reply(), is(anAppendReply().withResult(false)));
  }

  @Test
  public void repliesFalseIfItsLogDoesNotHaveAnEntryAtThePrevLogIndexSpecifiedInTheRequest() throws Exception {
    havingReceived(
        anAppendEntriesRequest()
            .withPrevLogTerm(4).withPrevLogIndex(1)
            .withEntry(aLogEntry()));

    assertThat(reply(), is(anAppendReply().withResult(false)));
  }

  @Test
  public void willReplyWithItsNextLogEntryIfItReceivesAnAppendRequestThatConflictsWithItsLog() throws Exception {
    final long termInLog = 3;
    final long termInMessage = 4;

    havingLogged(
        entries().term(termInLog).indexes(1, 2, 3));

    havingReceived(
        anAppendEntriesRequest()
            .withPrevLogTerm(termInMessage).withPrevLogIndex(10)
            .withEntries(entries().term(termInMessage).indexes(11, 12)));

    assertThat(reply(), is(
        anAppendReply()
            .withResult(false).withNextLogIndex(equalTo(4L))
    ));
  }

  @Test
  public void updatesAndPersistsCurrentTermIfItReceivesARequestWithANewerTerm() throws Exception {
    final long newerTerm = CURRENT_TERM + 1;

    context.checking(new Expectations() {{
      oneOf(persistence).writeCurrentTermAndVotedFor(QUORUM_ID, newerTerm, votedForNoOne());
    }});

    havingReceived(
        anAppendEntriesRequest()
            .withANewerTerm(newerTerm));

    assertThat(reply(), is(anAppendReply().withResult(true)));
    assertThat(latestIssuedElectionTerm(), is(equalTo(newerTerm)));
  }

  @Test
  public void appendsNewEntriesToTheLogIfThePrevLogIndexAndTermInTheRequestMatchItsLog() throws Exception {
    final long prevLogTerm = 4;
    final long prevLogIndex = 1;

    final List<LogEntry> receivedEntries = entries().term(prevLogTerm).indexes(2, 3).build();

    context.checking(new Expectations() {{
      oneOf(log).logEntries(receivedEntries);
    }});

    havingLogged(
        entries().term(prevLogTerm).indexes(prevLogIndex));

    havingReceived(
        anAppendEntriesRequest()
            .withPrevLogTerm(prevLogTerm).withPrevLogIndex(prevLogIndex)
            .withEntries(receivedEntries));

    assertThat(reply(), is(anAppendReply().withResult(true)));
  }

  @Test
  public void truncatesIfNecessaryBeforeAppendingNewlyReceivedEntries() throws Exception {
    final long prevLogTerm = 4;
    final long prevLogIndex = 1;

    final List<LogEntry> receivedEntries = entries().term(prevLogTerm + 1).indexes(2, 3, 4).build();

    context.checking(new Expectations() {{
      oneOf(log).truncateLog(firstIndexIn(receivedEntries));
      oneOf(log).logEntries(receivedEntries);
    }});

    havingLogged(
        entries().term(prevLogTerm).indexes(1, 2, 3, 4));

    havingReceived(
        anAppendEntriesRequest()
            .withPrevLogTerm(prevLogTerm).withPrevLogIndex(prevLogIndex)
            .withEntries(receivedEntries));

    assertThat(reply(), is(anAppendReply().withResult(true)));
  }

  @Test
  public void commitsIfItReceivesAnEmptyAppendEntriesRequestWithANewerCommitIndex() throws Exception {
    final long receivedCommitIndex = 3;

    havingLogged(
        entries().term(1).indexes(1, 2, 3, 4));

    havingReceived(
        anAppendEntriesRequest()
            .withPrevLogTerm(1).withPrevLogIndex(1)
            .withNoEntries()
            .withCommitIndex(receivedCommitIndex));

    assertThatReplicatorWillCommitUpToIndex(receivedCommitIndex);
  }

  @Test
  public void commitsIfItReceivesAnAppendEntriesRequestWithACommitIndexWithinTheEntriesSent() throws Exception {
    final List<LogEntry> receivedEntries = entries().term(1).indexes(1, 2, 3).build();
    final long receivedCommitIndex = 2;

    context.checking(new Expectations() {{
      oneOf(log).logEntries(receivedEntries);
    }});

    havingReceived(
        anAppendEntriesRequest()
            .withEntries(receivedEntries)
            .withCommitIndex(receivedCommitIndex));

    assertThatReplicatorWillCommitUpToIndex(receivedCommitIndex);
  }

  @Test
  public void issuesASeparateCommitNoticeForEachTermInTheRangeOfCommittedEntries() throws Exception {
    context.checking(new Expectations() {{
      allowing(log).logEntries(with(anyList()));
    }});

    havingReceived(
        anAppendEntriesRequest()
            .withEntries(entries()
                .term(101).indexes(1)
                .term(102).indexes(2, 3)
                .term(103).indexes(4, 5, 6))
            .withCommitIndex(5));

    assertThatReplicatorWillIssue(aCommitNotice()
        .withTerm(equalTo(101L)).withIndexRange(equalTo(1L), equalTo(1L)));

    assertThatReplicatorWillIssue(aCommitNotice()
        .withTerm(equalTo(102L)).withIndexRange(equalTo(2L), equalTo(3L)));

    assertThatReplicatorWillIssue(aCommitNotice()
        .withTerm(equalTo(103L)).withIndexRange(equalTo(4L), equalTo(5L)));
  }

  @Test
  public void willLogANewQuorumConfigurationItReceivesAndUpdateItsCurrentConfiguration() throws Exception {
    final QuorumConfiguration configuration = aNewConfiguration();
    final List<LogEntry> receivedEntries = entries()
        .term(1)
        .configurationAndIndex(configuration, 1)
        .build();

    context.checking(new Expectations() {{
      oneOf(log).logEntries(receivedEntries);
    }});

    havingReceived(
        anAppendEntriesRequest()
            .withEntries(receivedEntries));

    assertThat(reply(), is(anAppendReply().withResult(true)));
    assertThat(replicatorInstance.getQuorumConfiguration().get(), is(equalTo(configuration)));
  }

  @Test
  public void processesAppendRequestsAtomically() throws Exception {
    context.checking(new Expectations() {{
      oneOf(log).truncateLog(with(any(Long.class)));
      allowing(log).logEntries(with(anyList()));
    }});

    havingLogged(
        entries().term(101).indexes(1, 2, 3, 4));

    // This append request will require a both a log truncation and an append.
    havingReceived(anAppendEntriesRequest()
        .withEntries(entries().term(102).indexes(3, 4, 5))
        .withPrevLogTerm(101).withPrevLogIndex(2));

    // This append will (almost certainly) fail unless the previous append was processed atomically.
    havingReceived(anAppendEntriesRequest()
        .withEntries(entries().term(102).indexes(6, 7))
        .withPrevLogTerm(102).withPrevLogIndex(5));

    assertThat(reply(), is(anAppendReply().withResult(true)));
    assertThat(internalLog.getLogEntries(1, 8), resultsIn(aListOfEntriesWithConsecutiveSeqNums(1, 8)));
  }

  @Test
  public void ignoresEntriesThatAreIdenticalWithThoseAlreadyPresentInTheLog() throws Exception {
    context.checking(new Expectations() {{
      never(log).truncateLog(with(any(Long.class)));

      allowing(log).logEntries(with(anyList()));
    }});

    havingLogged(
        entries().term(101).indexes(1, 2, 3, 4));

    havingReceived(anAppendEntriesRequest()
        .withEntries(entries().term(101).indexes(3, 4, 5, 6))
        .withPrevLogTerm(101).withPrevLogIndex(2));

    assertThat(reply(), is(anAppendReply().withResult(true)));
    assertThat(internalLog.getLogEntries(1, 6), resultsIn(aListOfEntriesWithConsecutiveSeqNums(1, 6)));
  }

  private final Channel<IndexCommitNotice> commitNotices = new MemoryChannel<>();
  private final ChannelHistoryMonitor<IndexCommitNotice> commitMonitor =
      new ChannelHistoryMonitor<>(commitNotices, rpcFiber);

  private final Channel<ReplicatorInstanceEvent> eventChannel = new MemoryChannel<>();
  private final ChannelHistoryMonitor<ReplicatorInstanceEvent> eventMonitor =
      new ChannelHistoryMonitor<>(eventChannel, rpcFiber);

  private ReplicatorInstance makeTestInstance() throws Exception {
    long thisReplicatorId = 1;
    ReplicatorClock info = new InRamSim.StoppableClock(0, Integer.MAX_VALUE / 2L);
    ReplicatorLog proxyLog = getReplicatorLogWhichInvokesMock();

    return new ReplicatorInstance(new ThreadFiber(new RunnableExecutorImpl(batchExecutor), null, true),
        thisReplicatorId,
        QUORUM_ID,
        proxyLog,
        info,
        persistence,
        new MemoryRequestChannel<>(),
        eventChannel,
        commitNotices,
        State.FOLLOWER);
  }

  private ReplicatorLog getReplicatorLogWhichInvokesMock() {
    return (ReplicatorLog) Proxy.newProxyInstance(
        ReplicatorLog.class.getClassLoader(),
        new Class[]{ReplicatorLog.class},
        (proxy, method, args) -> {
          try {
            // Invoke mock log, allowing expectations to be satisfied
            method.invoke(log, args);
          } catch (InvocationTargetException e) {
            throw e.getTargetException();
          }
          // Invoke and return result from working (fake) log
          return method.invoke(internalLog, args);
        });
  }

  private long latestIssuedElectionTerm() {
    return eventMonitor.getLatest(leaderElectedEvent(any(Long.class), any(Long.class))).leaderElectedTerm;
  }

  private void assertThatReplicatorWillCommitUpToIndex(long index) {
    commitMonitor.waitFor(aCommitNotice().withIndex(greaterThanOrEqualTo(index)));
    assertFalse(commitMonitor.hasAny(aCommitNotice().withIndex(greaterThan(index))));
  }

  private void assertThatReplicatorWillIssue(Matcher<IndexCommitNotice> commitNoticeMatcher) {
    commitMonitor.waitFor(commitNoticeMatcher);
  }

  private SettableFuture<RpcReply> lastReply = null;

  private void havingReceived(AppendEntriesMessageBuilder messageBuilder) {
    lastReply = SettableFuture.create();
    final RpcWireRequest request = new RpcWireRequest(LEADER_ID, QUORUM_ID, messageBuilder.build());
    AsyncRequest.withOneReply(rpcFiber, replicatorInstance.getIncomingChannel(), request, lastReply::set);
  }

  private RpcReply reply() throws Exception {
    return lastReply.get(RPC_REPLY_TIMEOUT, TimeUnit.SECONDS);
  }

  private static AppendEntriesMessageBuilder anAppendEntriesRequest() {
    return new AppendEntriesMessageBuilder();
  }

  private static class AppendEntriesMessageBuilder {
    private long term = CURRENT_TERM;
    private long leaderId = LEADER_ID;
    private long prevLogIndex = 0;
    private long prevLogTerm = 0;
    private List<LogEntry> entries = new ArrayList<>();
    private long commitIndex = 0;

    public AppendEntriesMessageBuilder withAnOldTerm() {
      term = CURRENT_TERM - 1;
      return this;
    }

    public AppendEntriesMessageBuilder withANewerTerm(long term) {
      assert term > this.term;
      this.term = term;
      return this;
    }

    public AppendEntriesMessageBuilder withNoEntries() {
      entries = new ArrayList<>();
      return this;
    }

    public AppendEntriesMessageBuilder withEntry(LogEntry entry) {
      entries.add(entry);
      return this;
    }

    public AppendEntriesMessageBuilder withEntries(LogSequenceBuilder sequenceBuilder) {
      entries.addAll(sequenceBuilder.build());
      return this;
    }

    public AppendEntriesMessageBuilder withEntries(List<LogEntry> entries) {
      this.entries.addAll(entries);
      return this;
    }

    public AppendEntriesMessageBuilder withPrevLogTerm(long term) {
      prevLogTerm = term;
      return this;
    }

    public AppendEntriesMessageBuilder withPrevLogIndex(long index) {
      prevLogIndex = index;
      return this;
    }

    public AppendEntriesMessageBuilder withCommitIndex(long commitIndex) {
      this.commitIndex = commitIndex;
      return this;
    }

    public AppendEntries build() {
      return new AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex);
    }
  }

  private long nextLogIndex = aSeqNum();

  private LogEntry aLogEntry() {
    return makeProtostuffEntry(nextLogIndex++, CURRENT_TERM, someData());
  }

  private QuorumConfiguration aNewConfiguration() {
    return QuorumConfiguration.of(Lists.newArrayList(2L, 3L, 4L, 5L));
  }

  private final ReplicatorLog internalLog = new InRamLog();

  private void havingLogged(ReplicatorTestUtil.LogSequenceBuilder sequenceBuilder) throws Exception {
    List<LogEntry> entries = sequenceBuilder.build();
    internalLog.logEntries(entries).get();
  }

  private long firstIndexIn(List<LogEntry> entries) {
    return entries.get(0).getIndex();
  }

  private long votedForNoOne() {
    return 0;
  }

  private Matcher<List<LogEntry>> anyList() {
    return Matchers.instanceOf(List.class);
  }
}