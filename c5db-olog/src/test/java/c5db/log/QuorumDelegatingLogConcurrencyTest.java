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

package c5db.log;

import c5db.util.KeySerializingExecutor;
import c5db.util.WrappingKeySerializingExecutor;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static c5db.ConcurrencyTestUtil.runAConcurrencyTestSeveralTimes;
import static c5db.ConcurrencyTestUtil.runNTimesAndWaitForAllToComplete;
import static c5db.FutureMatchers.resultsIn;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.QuorumDelegatingLogUnitTest.ArrayPersistenceService;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class QuorumDelegatingLogConcurrencyTest {
  private static final int LOG_WORKER_THREADS = 8;
  private static final int ENTRIES_PER_QUORUM_PER_TEST = 1;

  @Test(timeout = 5000)
  public void isThreadSafeWithRespectToLoggingFromMultipleQuorumsWithEachQuorumItsOwnThread() throws Exception {
    final int numThreads = 150;
    final int numAttempts = 5;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, this::runMultipleQuorumThreadSafetyTest);
  }

  @Test(timeout = 5000)
  public void isThreadSafeWithRespectToCallingCloseFromMultipleThreads() throws Exception {
    final int numThreads = 100;
    final int numAttempts = 10;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, this::runCloseThreadSafetyTest);
  }


  private void runMultipleQuorumThreadSafetyTest(int numQuorums, ExecutorService executor) throws Exception {
    try (OLog log = createLog()) {
      runForNQuorums(numQuorums, executor, (quorumId) -> {
        log.openAsync(quorumId)
            .get();
        log.logEntries(logEntriesForQuorum(quorumId), quorumId)
            .get();
      });

      assertThatEverythingWasLoggedCorrectly(log);
    }
  }

  private void runCloseThreadSafetyTest(int numQuorums, ExecutorService executor) throws Exception {
    try (OLog log = createLog()) {
      runForNQuorums(numQuorums, executor, (quorumId) -> {
        log.openAsync(quorumId)
            .get();
        log.logEntries(logEntriesForQuorum(quorumId), quorumId)
            .get();
      });

      runNTimesAndWaitForAllToComplete(numQuorums * 2, executor, log::close);
    }
  }

  private void runForNQuorums(int numQuorums, ExecutorService executor, QuorumRunner runner) throws Exception {
    runNTimesAndWaitForAllToComplete(numQuorums, executor,
        (int invocationIndex) -> {
          String quorumId = getQuorumNameForIndex(invocationIndex);
          runner.run(quorumId);
        });
  }

  private void assertThatEverythingWasLoggedCorrectly(OLog log) {
    for (String quorumId : allQuorums()) {
      assertThat(log.getLogEntries(1, 1 + ENTRIES_PER_QUORUM_PER_TEST, quorumId),
          resultsIn(equalTo(logEntriesForQuorum(quorumId))));

      assertThat(log.getLogTerm(1, quorumId),
          is(equalTo(getTermAtSeq(1, quorumId))));
    }
  }

  private OLog createLog() {
    KeySerializingExecutor executor = new WrappingKeySerializingExecutor(newFixedThreadPool(LOG_WORKER_THREADS));

    // Run test in memory for speed. The concurrency properties of the code will still be tested.
    return new QuorumDelegatingLog(
        new ArrayPersistenceService(),
        executor,
        NavigableMapOLogEntryOracle::new,
        InMemoryPersistenceNavigator::new);
  }

  private String getQuorumNameForIndex(int index) {
    return "quorum" + String.valueOf(index);
  }

  private final Map<String, List<OLogEntry>> logEntryListsMemoized = new ConcurrentHashMap<>();

  private List<OLogEntry> logEntriesForQuorum(String quorumId) {
    logEntryListsMemoized.putIfAbsent(quorumId, someConsecutiveEntries(1, 1 + ENTRIES_PER_QUORUM_PER_TEST));
    return logEntryListsMemoized.get(quorumId);
  }

  private long getTermAtSeq(long seqNum, String quorumId) {
    for (OLogEntry entry : logEntryListsMemoized.get(quorumId)) {
      if (entry.getSeqNum() == seqNum) {
        return entry.getElectionTerm();
      }
    }
    throw new IndexOutOfBoundsException("getTermAtSeq");
  }

  private Set<String> allQuorums() {
    return logEntryListsMemoized.keySet();
  }

  private interface QuorumRunner {
    void run(String quorumId) throws Exception;
  }
}
