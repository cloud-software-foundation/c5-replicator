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

import c5db.C5CommonTestUtil;
import c5db.interfaces.LogModule;
import c5db.interfaces.log.Reader;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.replication.generated.LogEntry;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import c5db.util.JUnitRuleFiberExceptions;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

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


  private <E extends SequentialEntry> SequentialEntryIterator<E> iteratorOfFirstLogInReader(Reader<E> reader)
      throws Exception {
    return reader.getLogList().get(0).get();
  }

  private void havingLogged(List<LogEntry> entries) throws Exception {
    log.logEntries(entries).get();
  }

  private List<OLogEntry> oLogEntries(List<LogEntry> entries) {
    return entries.stream()
        .map(OLogEntry::fromProtostuff)
        .collect(Collectors.toList());
  }

  private List<LogEntry> someConsecutiveLogEntries() {
    return entries()
        .term(7)
        .indexes(1, 2, 3, 4, 5, 6)
        .build();
  }

  private FiberSupplier makeFiberSupplier() {
    return (throwableHandler) -> {
      BatchExecutor batchExecutor = new ExceptionHandlingBatchExecutor(throwableHandler);
      RunnableExecutor runnableExecutor = new RunnableExecutorImpl(batchExecutor);
      return new ThreadFiber(runnableExecutor, "o-log-reader-test-thread", false);
    };
  }
}
