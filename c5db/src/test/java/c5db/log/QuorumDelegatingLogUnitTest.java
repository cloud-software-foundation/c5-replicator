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
import c5db.util.KeySerializingExecutorTest;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static c5db.log.EncodedSequentialLog.Codec;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.term;

@SuppressWarnings("unchecked")
public class QuorumDelegatingLogUnitTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  LogPersistenceService persistenceService = context.mock(LogPersistenceService.class);
  ExecutorService executorService = context.mock(ExecutorService.class);
  KeySerializingExecutor serializingExecutor = new KeySerializingExecutor(executorService); // not a mock!
  Supplier<TermOracle> termOracleFactory = context.mock(Supplier.class);
  BiFunction<BytePersistence, Codec, PersistenceNavigator> navigatorFactory = context.mock(BiFunction.class);
  TermOracle termOracle = context.mock(TermOracle.class);
  PersistenceNavigator persistenceNavigator = context.mock(PersistenceNavigator.class);

  QuorumDelegatingLog oLog = new QuorumDelegatingLog(
      persistenceService,
      serializingExecutor,
      termOracleFactory,
      navigatorFactory);

  @Before
  public void setUpMockedFactories() {
    context.checking(new Expectations() {{
      allowing(navigatorFactory).apply(with(any(BytePersistence.class)), with(any(Codec.class)));
      will(returnValue(persistenceNavigator));

      allowing(termOracleFactory).get();
      will(returnValue(termOracle));
    }});
  }

  @Test
  public void getsOneNewPersistenceObjectPerQuorumWhenLogEntriesIsCalled() throws Exception {
    String quorumA = "quorumA";
    String quorumB = "quorumB";

    context.checking(new Expectations() {{
      ignoring(executorService);
      allowing(termOracle).notifyLogging(with(any(Long.class)), with(any(Long.class)));

      oneOf(persistenceService).getPersistence(quorumA);
      oneOf(persistenceService).getPersistence(quorumB);
    }});

    oLog.logEntry(arbitraryEntries(), quorumA);
    oLog.logEntry(arbitraryEntries(), quorumB);
  }

  @Test
  public void passesLogRequestsAsTasksToItsExecutorService() throws Exception {
    String quorumId = "quorum";

    context.checking(new Expectations() {{
      ignoring(termOracle);
      ignoring(persistenceService);
      KeySerializingExecutorTest.allowSubmitOrExecuteOnce(context, executorService);
    }});

    oLog.logEntry(arbitraryEntries(), quorumId);
  }

  private static List<OLogEntry> arbitraryEntries() {
    return makeSingleEntryList(seqNum(1), term(1), "x");
  }
}
