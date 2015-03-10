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

import c5db.C5CommonTestUtil;
import c5db.interfaces.LogModule;
import c5db.interfaces.log.Reader;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorEntry;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.replication.generated.LogEntry;
import c5db.util.Consumer;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Sets;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static c5db.interfaces.log.SequentialEntryIterable.SequentialEntryIterator;
import static c5db.interfaces.log.SequentialEntryIterableMatchers.isIteratorContainingInOrder;
import static c5db.replication.ReplicatorTestUtil.entries;
import static org.hamcrest.MatcherAssert.assertThat;

public class OLogReaderTest {
  @Rule
  public JUnitRuleFiberExceptions throwableHandler = new JUnitRuleFiberExceptions();

  private static final String QUORUM_ID = "q";
  private final Path baseTestPath = new C5CommonTestUtil().getDataTestDir("o-log-reader-test");
  private final FiberSupplier fiberSupplier = makeFiberSupplier();
  private final LogModule logModule = new LogService(baseTestPath, fiberSupplier);

  // initialized after module starts
  private ReplicatorLog log;

  @Before
  public void startLogModule() throws Exception {
    logModule.startAndWait();
    log = logModule.getReplicatorLog(QUORUM_ID).get();
  }

  @After
  public void shutDownModule() throws Exception {
    logModule.stopAndWait();
  }

  @Test(timeout = 3000)
  public void iteratesOverLoggedEntriesWithinASinglePersistence() throws Exception {
    List<LogEntry> entries = someConsecutiveLogEntries();

    havingLogged(entries);

    Reader<OLogEntry> reader = logModule.getLogReader(QUORUM_ID, new OLogEntry.Codec());

    try (SequentialEntryIterator<OLogEntry> iterator = iteratorOfFirstLogInReader(reader)) {
      assertThat(iterator, isIteratorContainingInOrder(oLogEntries(entries)));
    }
  }

  @Test(timeout = 3000)
  public void iteratesOverLoggedEntriesWithAnIteratorThatSkipsQuorumConfigurationEntries() throws Exception {
    List<LogEntry> entries = someEntriesInterspersedWithConfigurationEntries();

    havingLogged(entries);

    Reader<ReplicatorEntry> reader = logModule.getLogReader(QUORUM_ID, new OLogToReplicatorEntryCodec());

    try (SequentialEntryIterator<ReplicatorEntry> iterator = iteratorOfFirstLogInReader(reader)) {
      assertThat(iterator, isIteratorContainingInOrder(justDataEntries(entries)));
    }
  }


  private <E extends SequentialEntry> SequentialEntryIterator<E> iteratorOfFirstLogInReader(Reader<E> reader)
      throws Exception {
    return reader.getLogList().get(0).get();
  }

  private void havingLogged(List<LogEntry> entries) throws Exception {
    log.logEntries(entries).get();
  }

  private List<OLogEntry> oLogEntries(List<LogEntry> entries) {
    List<OLogEntry> result = new ArrayList<>();
    for (LogEntry entry : entries) {
      result.add(OLogEntry.fromProtostuff(entry));
    }
    return result;
  }

  private List<ReplicatorEntry> justDataEntries(List<LogEntry> entries) {
    List<ReplicatorEntry> result = new ArrayList<>();
    for (LogEntry entry : entries) {
      if (entry.getQuorumConfiguration() == null) {
        result.add(new ReplicatorEntry(entry.getIndex(), entry.getDataList()));
      }
    }
    return result;
  }

  private List<LogEntry> someConsecutiveLogEntries() {
    return entries()
        .term(7)
        .indexes(1, 2, 3, 4, 5, 6)
        .build();
  }

  private List<LogEntry> someEntriesInterspersedWithConfigurationEntries() {
    return entries()
        .term(777)
        .configurationAndSeqNum(someConfiguration(), 1)
        .seqNums(2, 3)
        .configurationAndSeqNum(someConfiguration(), 4)
        .seqNums(5)
        .build();
  }

  private QuorumConfiguration someConfiguration() {
    return QuorumConfiguration.of(Sets.newHashSet(1L, 2L, 3L));
  }

  private FiberSupplier makeFiberSupplier() {
    return new FiberSupplier() {
      @Override
      public Fiber getNewFiber(Consumer<Throwable> throwableHandler) {
        BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(throwableHandler);
        RunnableExecutor runnableExecutor = new RunnableExecutorImpl(batchExecutor);
        return new ThreadFiber(runnableExecutor, "o-log-reader-test-thread", false);
      }
    };
  }
}
