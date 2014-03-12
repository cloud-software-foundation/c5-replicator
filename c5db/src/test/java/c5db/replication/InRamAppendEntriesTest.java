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
import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.LogEntry;
import c5db.replication.rpc.RpcReply;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.ThrowFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static c5db.interfaces.ReplicationModule.IndexCommitNotice;
import static c5db.replication.InRamTestUtil.syncSendMessage;
import static c5db.replication.ReplicatorInstance.State;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A class for testing a single ReplicatorInstance node, to determine if it reacts correctly to AppendEntries
 * messages.
 */
public class InRamAppendEntriesTest {
  private ReplicatorInstance repl;
  private Fiber rpcFiber;

  // Parameters common to all tests
  private static final long PEER_ID = 1;
  private static final long LEADER_ID = 2;
  private static final long CURRENT_TERM = 4;
  private static final List<LogEntry> TEST_ENTRIES = ImmutableList.of(
      new LogEntry(2, 1, null),
      new LogEntry(2, 2, null),
      new LogEntry(2, 3, null),
      new LogEntry(3, 4, null),
      new LogEntry(3, 5, null));
  private static final List<LogEntry> EMPTY_ENTRY_LIST = ImmutableList.of();

  Channel<ReplicationModule.IndexCommitNotice> commitNotices = new MemoryChannel<>();
  BlockingQueue<IndexCommitNotice> commits = new LinkedBlockingQueue<>();
  ReplicatorLogAbstraction log = new InRamLog();

  @Rule
  public ThrowFiberExceptions fiberExceptionHandler = new ThrowFiberExceptions(this);

  private RunnableExecutor runnableExecutor = new RunnableExecutorImpl(
      new ExceptionHandlingBatchExecutor(fiberExceptionHandler));

  private ReplicatorInstance makeTestInstance() {
    final List<Long> peerIdList = ImmutableList.of(1L, 2L, 3L);
    ReplicatorInformationInterface info = new TestableInRamSim.Info(0) {
      @Override
      public long electionTimeout() {
        return Long.MAX_VALUE / 2L; // Prevent election timeouts.
      }
    };

    return new ReplicatorInstance(new ThreadFiber(runnableExecutor, null, true),
        PEER_ID,
        "quorumId",
        peerIdList,
        log,
        info,
        new TestableInRamSim.Persister(),
        new MemoryRequestChannel<>(),
        new MemoryChannel<>(),
        commitNotices,
        CURRENT_TERM,
        State.FOLLOWER,
        0,
        LEADER_ID,
        0);
  }

  private void verifyCommitUpTo(long index) throws Exception {
    IndexCommitNotice lastCommitNotice;
    do {
      lastCommitNotice = commits.poll(5, TimeUnit.SECONDS);
      assertNotNull(lastCommitNotice);
    } while (lastCommitNotice.committedIndex < index);

    assertEquals(index, lastCommitNotice.committedIndex);
    assertTrue(commits.isEmpty());
  }

  @Before
  public final void setUp() {
    repl = makeTestInstance();
    repl.start();
    rpcFiber = new ThreadFiber(runnableExecutor, null, true);
    rpcFiber.start();
    commitNotices.subscribe(rpcFiber, commits::add);
  }

  @After
  public final void tearDown() {
    repl.dispose();
    repl = null;
    rpcFiber.dispose();
    rpcFiber = null;
  }

  @Test
  public void testReplyToOldTerm() throws Exception {
    // "Reply false if term < currentTerm (§5.1)"

    long messageTerm = CURRENT_TERM - 1;
    LogEntry entry = new LogEntry(CURRENT_TERM, 1, null);

    // Case with a nonempty entry list
    RpcReply reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, 0, 0, Lists.newArrayList(entry), 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(CURRENT_TERM, repl.currentTerm);
    assertEquals(0, log.getLastIndex());

    // Case with an empty entry list
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, 0, 0, EMPTY_ENTRY_LIST, 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(CURRENT_TERM, repl.currentTerm);
  }

  @Test
  public void testPrevLogIndexTermMismatch() throws Exception {
    // "Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)"
    // In this test case, the index is found, but it has the wrong term.
    log.logEntries(TEST_ENTRIES);

    long messageTerm = CURRENT_TERM;
    long prevLogIndex = 4;
    long prevLogTerm = 2; // This term conflicts with what's in the log.
    LogEntry entry = new LogEntry(CURRENT_TERM, 6, null);

    // Case with a nonempty entry list
    RpcReply reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, Lists.newArrayList(entry), 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(5, log.getLastIndex());

    // Case with an empty entry list
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, EMPTY_ENTRY_LIST, 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(5, log.getLastIndex());
  }

  @Test
  public void testPrevLogIndexEmptyLog() throws Exception {
    // "Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)"
    // In this case, the log contains no entries at all.
    assert log.getLastIndex() == 0;

    long messageTerm = CURRENT_TERM;
    long prevLogIndex = 1; // Log is empty, so this index isn't in it.
    long prevLogTerm = 1;
    LogEntry entry = new LogEntry(1, 1, null);

    RpcReply reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, Lists.newArrayList(entry), 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());

    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, EMPTY_ENTRY_LIST, 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
  }

  @Test
  public void testPrevLogIndexMissing() throws Exception {
    // "Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)"
    // In this case, the prevLogIndex is missing entirely from the replicator's log.
    log.logEntries(TEST_ENTRIES);

    long messageTerm = CURRENT_TERM;
    long prevLogIndex = 6; // Not in the log...
    long prevLogTerm = 4;

    // Case where the prevLogIndex is in the entry being sent, but not in the existing log
    LogEntry entry = new LogEntry(CURRENT_TERM, 6, null);
    RpcReply reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, Lists.newArrayList(entry), 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(5, log.getLastIndex());

    // Case where the prevLogIndex isn't anywhere
    entry = new LogEntry(CURRENT_TERM, 7, null);
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, Lists.newArrayList(entry), 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(5, log.getLastIndex());

    // Case with an empty entry list
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, EMPTY_ENTRY_LIST, 0));
    assertFalse(reply.getAppendReplyMessage().getSuccess());
    assertEquals(5, log.getLastIndex());
  }

  @Test
  public void testTermConflictAtMostRecentLogEntry() throws Exception {
    // "If an existing entry conflicts with a new one (same index but different terms),
    // delete the existing entry and all that follow it (§5.3)"
    // This tests the case where the conflict is the most recent entry.
    log.logEntries(TEST_ENTRIES);

    long messageTerm = CURRENT_TERM;
    long prevLogTerm = 3;
    long prevLogIndex = 4; // Note -- the prevLogTerm and prevLogIndex match with what's in the log.
    LogEntry entry = new LogEntry(CURRENT_TERM, 5, null); // conflict

    syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, Lists.newArrayList(entry), 0));

    assertNotEquals(3, log.getLogEntry(5).get().getTerm());
    assertEquals(4, log.getLogEntry(5).get().getTerm());
    assertEquals(5, log.getLastIndex());
    assertEquals(4, log.getLastTerm());
  }

  @Test
  public void testTermConflictAtPreviousEntry() throws Exception {
    // "If an existing entry conflicts with a new one (same index but different terms),
    // delete the existing entry and all that follow it (§5.3)"
    // This tests the case where the conflict is not the most recent entry.
    log.logEntries(TEST_ENTRIES);

    long messageTerm = CURRENT_TERM;
    long prevLogTerm = 3;
    long prevLogIndex = 4; // Note -- the prevLogTerm and prevLogIndex match with what's in the log.
    LogEntry entry = new LogEntry(CURRENT_TERM, 3, null); // conflict

    syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, Lists.newArrayList(entry), 0));

    assertNotEquals(2, log.getLogEntry(3).get().getTerm());
    assertEquals(4, log.getLogEntry(3).get().getTerm());
    assertNull(log.getLogEntry(4).get());
    assertNull(log.getLogEntry(5).get());
    assertEquals(3, log.getLastIndex());
    assertEquals(4, log.getLastTerm());
  }

  @Test
  public void testCommitIndexUpdate() throws Exception {
    // "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)"
    log.logEntries(TEST_ENTRIES);

    assert commits.isEmpty();
    assert log.getLastIndex() == 5;

    long messageTerm = CURRENT_TERM;
    long prevLogTerm = 3;
    long prevLogIndex = 4;

    // Situation where the append entries message contains no entries itself.
    RpcReply reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, EMPTY_ENTRY_LIST, 1));
    assert reply.getAppendReplyMessage().getSuccess();
    verifyCommitUpTo(1);

    // Send two more entries and advance commit index to 3
    List<LogEntry> entries = ImmutableList.of(
        new LogEntry(CURRENT_TERM, 5, null),
        new LogEntry(CURRENT_TERM, 6, null));
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, entries, 3));
    assertTrue(reply.getAppendReplyMessage().getSuccess());
    verifyCommitUpTo(3);
    assertEquals(CURRENT_TERM, log.getLogTerm(5));
    assertEquals(CURRENT_TERM, log.getLogTerm(6));
    assertEquals(6, log.getLastIndex());

    // Send a commit index beyond currently logged entries
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, EMPTY_ENTRY_LIST, 7));
    assertTrue(reply.getAppendReplyMessage().getSuccess());
    verifyCommitUpTo(6); // Not 7, because there are only 6 log entries.

    // Send a commit index within current entries being sent
    entries = ImmutableList.of(
        new LogEntry(CURRENT_TERM, 7, null),
        new LogEntry(CURRENT_TERM, 8, null));
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, entries, 7));
    assertTrue(reply.getAppendReplyMessage().getSuccess());
    verifyCommitUpTo(7);

    // Send a commit index beyond current entries being sent
    entries = ImmutableList.of(
        new LogEntry(CURRENT_TERM + 1, 8, null),
        new LogEntry(CURRENT_TERM + 1, 9, null));
    reply = syncSendMessage(rpcFiber, LEADER_ID, repl,
        new AppendEntries(messageTerm, LEADER_ID, prevLogIndex, prevLogTerm, entries, 11));
    assertTrue(reply.getAppendReplyMessage().getSuccess());
    verifyCommitUpTo(9);
    assertEquals(CURRENT_TERM + 1, log.getLogTerm(8));
    assertEquals(9, log.getLastIndex());
    assertEquals(CURRENT_TERM + 1, log.getLastTerm());
  }
}