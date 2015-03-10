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

package c5db.log;

import c5db.ConcurrencyTestUtil;
import c5db.interfaces.log.SequentialEntryCodec;
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

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, new ConcurrencyTestUtil.ConcurrencyTest() {
      @Override
      public void run(int numQuorums, ExecutorService executor) throws Exception {
        QuorumDelegatingLogConcurrencyTest.this.runMultipleQuorumThreadSafetyTest(numQuorums, executor);
      }
    });
  }

  @Test(timeout = 5000)
  public void isThreadSafeWithRespectToCallingCloseFromMultipleThreads() throws Exception {
    final int numThreads = 100;
    final int numAttempts = 10;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, new ConcurrencyTestUtil.ConcurrencyTest() {
      @Override
      public void run(int numQuorums, ExecutorService executor) throws Exception {
        QuorumDelegatingLogConcurrencyTest.this.runCloseThreadSafetyTest(numQuorums, executor);
      }
    });
  }


  private void runMultipleQuorumThreadSafetyTest(int numQuorums, ExecutorService executor) throws Exception {
    try (OLog log = createLog()) {
      runForNQuorums(numQuorums, executor, new QuorumRunner() {
        @Override
        public void run(String quorumId) throws Exception {
          log.openAsync(quorumId)
              .get();
          log.logEntries(QuorumDelegatingLogConcurrencyTest.this.logEntriesForQuorum(quorumId), quorumId)
              .get();
        }
      });

      assertThatEverythingWasLoggedCorrectly(log);
    }
  }

  private void runCloseThreadSafetyTest(int numQuorums, ExecutorService executor) throws Exception {
    try (OLog log = createLog()) {
      runForNQuorums(numQuorums, executor, new QuorumRunner() {
        @Override
        public void run(String quorumId) throws Exception {
          log.openAsync(quorumId)
              .get();
          log.logEntries(QuorumDelegatingLogConcurrencyTest.this.logEntriesForQuorum(quorumId), quorumId)
              .get();
        }
      });

      runNTimesAndWaitForAllToComplete(numQuorums * 2, executor, new ConcurrencyTestUtil.ExceptionThrowingRunnable() {
        @Override
        public void run() throws Exception {
          log.close();
        }
      });
    }
  }

  private void runForNQuorums(int numQuorums, ExecutorService executor, final QuorumRunner runner) throws Exception {
    runNTimesAndWaitForAllToComplete(numQuorums, executor,
        new ConcurrencyTestUtil.IndexedExceptionThrowingRunnable() {
          @Override
          public void run(int invocationIndex) throws Exception {
            String quorumId = QuorumDelegatingLogConcurrencyTest.this.getQuorumNameForIndex(invocationIndex);
            runner.run(quorumId);
          }
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
        (OLogEntryOracle.OLogEntryOracleFactory) new OLogEntryOracle.OLogEntryOracleFactory() {
          @Override
          public OLogEntryOracle create() {
            return new NavigableMapOLogEntryOracle();
          }
        },
        new LogPersistenceService.PersistenceNavigatorFactory() {
          @Override
          public LogPersistenceService.PersistenceNavigator create(LogPersistenceService.BytePersistence persistence, SequentialEntryCodec<?> encoding, long offset) {
            return new InMemoryPersistenceNavigator<>(persistence, encoding, offset);
          }
        });
  }

  private String getQuorumNameForIndex(int index) {
    return "quorum" + String.valueOf(index);
  }

  private final Map<String, List<OLogEntry>> logEntryListsMemoized = new ConcurrentHashMap<>();

  private List<OLogEntry> logEntriesForQuorum(String quorumId) {
    synchronized (logEntryListsMemoized) {
      if (!logEntryListsMemoized.containsKey(quorumId)) {
        logEntryListsMemoized.put(quorumId, someConsecutiveEntries(1, 1 + ENTRIES_PER_QUORUM_PER_TEST));
      }
    }
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
