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

import c5db.interfaces.log.SequentialEntryCodec;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.util.CheckedSupplier;
import c5db.util.KeySerializingExecutor;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static c5db.FutureMatchers.resultsInException;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceNavigatorFactory;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.OLog.QuorumNotOpen;
import static c5db.log.OLogEntryOracle.OLogEntryOracleFactory;
import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.seqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.someData;
import static c5db.log.ReplicatorLogGenericTestUtil.term;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@SuppressWarnings("unchecked")
public class QuorumDelegatingLogUnitTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final KeySerializingExecutor serializingExecutor =
      new WrappingKeySerializingExecutor(MoreExecutors.sameThreadExecutor());

  private final OLogEntryOracleFactory OLogEntryOracleFactory = context.mock(OLogEntryOracleFactory.class);
  private final OLogEntryOracle oLogEntryOracle = context.mock(OLogEntryOracle.class);

  private final PersistenceNavigatorFactory navigatorFactory = context.mock(PersistenceNavigatorFactory.class);
  private final PersistenceNavigator persistenceNavigator = context.mock(PersistenceNavigator.class);

  private final ArrayPersistenceService persistenceService = new ArrayPersistenceService();
  private final QuorumDelegatingLog oLog = new QuorumDelegatingLog(
      persistenceService,
      serializingExecutor,
      OLogEntryOracleFactory,
      navigatorFactory);

  @Before
  public void setUpMockedFactories() throws Exception {
    context.checking(new Expectations() {{
      allowing(navigatorFactory).create(with(any(BytePersistence.class)),
          with.<SequentialEntryCodec<?>>is(any(SequentialEntryCodec.class)),
          with(any(Long.class)));
      will(returnValue(persistenceNavigator));

      allowing(OLogEntryOracleFactory).create();
      will(returnValue(oLogEntryOracle));

      atMost(1).of(oLogEntryOracle).notifyLogging(with(any(OLogEntry.class)));

      allowing(oLogEntryOracle).getGreatestSeqNum();

      allowing(persistenceNavigator).getStreamAtFirstEntry();
      will(returnValue(aZeroLengthInputStream()));
    }});
  }

  @After
  public void closeLog() throws Exception {
    oLog.close();
  }

  @Test(expected = QuorumNotOpen.class)
  public void throwsAnExceptionIfAttemptingToLogToAQuorumBeforeOpeningIt() throws Exception {
    oLog.logEntries(arbitraryEntries(), "quorum");
  }

  @Test(expected = Exception.class)
  public void throwsAnExceptionIfAttemptingToOpenAQuorumAfterClosingTheLog() throws Exception {
    oLog.close();
    oLog.openAsync("quorum");
  }

  @Test(timeout = 3000)
  public void getsOneNewPersistenceObjectPerQuorumWhenLogEntriesIsCalled() throws Exception {
    String quorumA = "quorumA";
    String quorumB = "quorumB";

    context.checking(new Expectations() {{
      allowing(oLogEntryOracle).notifyLogging(with(any(OLogEntry.class)));
      allowing(persistenceNavigator).notifyLogging(with(any(Long.class)), with(any(Long.class)));
      allowing(persistenceNavigator).addToIndex(with(any(Long.class)), with(any(Long.class)));
    }});

    oLog.openAsync(quorumA).get();
    oLog.openAsync(quorumB).get();

    oLog.logEntries(arbitraryEntries(), quorumA);
    oLog.logEntries(arbitraryEntries(), quorumB);
  }

  @Test(timeout = 3000)
  public void passesLoggedEntriesToItsOLogEntryOracleObject() throws Exception {
    final String quorumId = "quorum";
    final OLogEntry entry = makeEntry(seqNum(1), term(1), someData());

    context.checking(new Expectations() {{
      ignoring(persistenceNavigator);

      oneOf(oLogEntryOracle).notifyLogging(entry);
    }});

    oLog.openAsync(quorumId).get();
    oLog.logEntries(Lists.newArrayList(entry), quorumId);
  }

  @Test(timeout = 3000)
  public void createsANewLogWhenRollIsCalledAndWritesSubsequentEntriesToTheNewLog() throws Exception {
    final String quorumId = "quorum";

    context.checking(new Expectations() {{
      ignoring(persistenceNavigator);

      allowing(oLogEntryOracle).notifyLogging(with(any(OLogEntry.class)));

      allowing(oLogEntryOracle).getLastTerm();
      allowing(oLogEntryOracle).getLastQuorumConfig();
      will(returnValue(new QuorumConfigurationWithSeqNum(QuorumConfiguration.EMPTY, 0)));
    }});

    oLog.openAsync(quorumId).get();
    oLog.logEntries(someConsecutiveEntries(1, 11), quorumId);
    oLog.roll(quorumId).get();

    assertThat(logPersistenceObjectsForQuorum(quorumId), hasSize(2));

    deleteFirstLog(quorumId);
    oLog.logEntries(someConsecutiveEntries(11, 21), quorumId).get();
  }

  @Test(timeout = 3000)
  public void returnsAnExceptionIfAttemptingToTruncateBackToANonExistentLog() throws Exception {
    final String quorumId = "quorum";

    context.checking(new Expectations() {{
      ignoring(persistenceNavigator);

      allowing(oLogEntryOracle).notifyLogging(with(any(OLogEntry.class)));
      allowing(oLogEntryOracle).notifyTruncation(with(any(Long.class)));
      allowing(oLogEntryOracle).getLastTerm();

      allowing(oLogEntryOracle).getLastQuorumConfig();
      will(returnValue(new QuorumConfigurationWithSeqNum(QuorumConfiguration.EMPTY, 0)));
    }});

    oLog.openAsync(quorumId).get();
    oLog.logEntries(someConsecutiveEntries(1, 11), quorumId);
    oLog.roll(quorumId).get();

    deleteFirstLog(quorumId);
    assertThat(oLog.truncateLog(seqNum(5), quorumId), resultsInException(IOException.class));
  }

  private static List<OLogEntry> arbitraryEntries() {
    return makeSingleEntryList(seqNum(1), term(1), "x");
  }

  private InputStream aZeroLengthInputStream() {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };
  }

  private Collection<BytePersistence> logPersistenceObjectsForQuorum(String quorumId) {
    return new ArrayList<>(persistenceService.quorumMap.get(quorumId));
  }

  private void deleteFirstLog(String quorumId) throws Exception {
    // TODO for now, closing the persistence, which is actually in-memory and not really persisted
    // TODO  to any medium, is used to simulate the underlying persistence having been deleted.
    persistenceService.firstLog(quorumId).close();
  }

  /**
   * In-memory LogPersistenceService to simplify these tests, rather than use mocks that return mocks.
   */
  private static class ArrayPersistenceService implements LogPersistenceService<ByteArrayPersistence> {
    private final Map<String, Deque<ByteArrayPersistence>> quorumMap = new HashMap<>();

    @Nullable
    @Override
    public ByteArrayPersistence getCurrent(String quorumId) throws IOException {
      quorumMap.putIfAbsent(quorumId, new LinkedList<>());
      return quorumMap.get(quorumId).peek();
    }

    @NotNull
    @Override
    public ByteArrayPersistence create(String quorumId) throws IOException {
      return new ByteArrayPersistence();
    }

    @Override
    public void append(String quorumId, @NotNull ByteArrayPersistence persistence) throws IOException {
      quorumMap.putIfAbsent(quorumId, new LinkedList<>());
      quorumMap.get(quorumId).push(persistence);
    }

    @Override
    public void truncate(String quorumId) throws IOException {
      quorumMap.get(quorumId).pop();
    }

    @Override
    public ImmutableList<CheckedSupplier<ByteArrayPersistence, IOException>> getList(String quorumId)
        throws IOException {
      List<CheckedSupplier<ByteArrayPersistence, IOException>> persistenceSupplierList = new ArrayList<>();

      for (ByteArrayPersistence persistence : quorumMap.get(quorumId)) {
        persistenceSupplierList.add(
            () -> persistence);
      }

      return ImmutableList.copyOf(persistenceSupplierList);
    }

    public BytePersistence firstLog(String quorumId) {
      return quorumMap.get(quorumId).peekLast();
    }
  }
}
