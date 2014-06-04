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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceNavigatorFactory;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.someData;
import static c5db.log.LogTestUtil.term;
import static c5db.log.OLog.QuorumNotOpen;
import static c5db.log.OLogEntryOracle.OLogEntryOracleFactory;

@SuppressWarnings("unchecked")
public class QuorumDelegatingLogUnitTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  private final LogPersistenceService persistenceService = context.mock(LogPersistenceService.class);
  private final BytePersistence bytePersistence = context.mock(BytePersistence.class);

  private final KeySerializingExecutor serializingExecutor =
      new WrappingKeySerializingExecutor(MoreExecutors.sameThreadExecutor());

  private final OLogEntryOracleFactory OLogEntryOracleFactory = context.mock(OLogEntryOracleFactory.class);
  private final OLogEntryOracle oLogEntryOracle = context.mock(OLogEntryOracle.class);

  private final PersistenceNavigatorFactory navigatorFactory = context.mock(PersistenceNavigatorFactory.class);
  private final PersistenceNavigator persistenceNavigator = context.mock(PersistenceNavigator.class);


  private final QuorumDelegatingLog oLog = new QuorumDelegatingLog(
      persistenceService,
      serializingExecutor,
      OLogEntryOracleFactory,
      navigatorFactory);

  @Before
  public void setUpMockedFactories() throws Exception {
    context.checking(new Expectations() {{
      allowing(navigatorFactory).create(with(any(BytePersistence.class)),
          with.<SequentialEntryCodec<?>>is(any(SequentialEntryCodec.class)));
      will(returnValue(persistenceNavigator));

      allowing(OLogEntryOracleFactory).create();
      will(returnValue(oLogEntryOracle));

      allowing(bytePersistence).isEmpty();
      will(returnValue(true));

      allowing(bytePersistence).size();
      will(returnValue(0L));

      allowing(bytePersistence).close();
    }});
  }

  @After
  public void closeLog() throws Exception {
    oLog.close();
  }

  @Test(expected = QuorumNotOpen.class)
  public void throwsAnExceptionIfAttemptingToLogToAQuorumBeforeOpeningIt() throws Exception {
    oLog.logEntry(arbitraryEntries(), "quorum");
  }

  @Test(expected = Exception.class)
  public void throwsAnExceptionIfAttemptingToOpenAQuorumAfterClosingTheLog() throws Exception {
    oLog.close();
    oLog.openAsync("quorum");
  }

  @Test
  public void getsOneNewPersistenceObjectPerQuorumWhenLogEntriesIsCalled() throws Exception {
    String quorumA = "quorumA";
    String quorumB = "quorumB";

    context.checking(new Expectations() {{
      allowing(oLogEntryOracle).notifyLogging(with(any(OLogEntry.class)));
      allowing(persistenceNavigator).notifyLogging(with(any(Long.class)), with(any(Long.class)));
      allowing(bytePersistence).append(with(any(ByteBuffer[].class)));

      oneOf(persistenceService).getPersistence(quorumA);
      will(returnValue(bytePersistence));

      oneOf(persistenceService).getPersistence(quorumB);
      will(returnValue(bytePersistence));
    }});

    oLog.openAsync(quorumA).get();
    oLog.openAsync(quorumB).get();

    oLog.logEntry(arbitraryEntries(), quorumA);
    oLog.logEntry(arbitraryEntries(), quorumB);
  }

  @Test
  public void passesLoggedEntriesToItsOLogEntryOracleObject() throws Exception {
    final String quorumId = "quorum";
    final OLogEntry entry = makeEntry(seqNum(1), term(1), someData());

    context.checking(new Expectations() {{
      ignoring(persistenceNavigator);

      allowing(persistenceService).getPersistence(quorumId);
      will(returnValue(bytePersistence));

      allowing(bytePersistence).append(with(any(ByteBuffer[].class)));

      oneOf(oLogEntryOracle).notifyLogging(entry);
    }});

    oLog.openAsync(quorumId).get();
    oLog.logEntry(Lists.newArrayList(entry), quorumId);
  }

  private static List<OLogEntry> arbitraryEntries() {
    return makeSingleEntryList(seqNum(1), term(1), "x");
  }
}
