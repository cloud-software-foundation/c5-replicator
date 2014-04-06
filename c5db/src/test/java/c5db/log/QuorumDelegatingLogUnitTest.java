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
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

import static c5db.log.LogTestUtil.makeSingleEntryList;

public class QuorumDelegatingLogUnitTest {
  Mockery context = new JUnit4Mockery();
  LogPersistenceService persistenceService = context.mock(LogPersistenceService.class);
  ExecutorService executorService = context.mock(ExecutorService.class);
  KeySerializingExecutor serializingExecutor = new KeySerializingExecutor(executorService);
  QuorumDelegatingLog oLog = new QuorumDelegatingLog(persistenceService, serializingExecutor);

  @Test
  public void getsNewPersistenceObjectWhenLogEntriesIsCalled() throws Exception {
    String quorumA = "quorumA";
    String quorumB = "quorumB";

    context.checking(new Expectations() {{
      ignoring(executorService);
      atLeast(1).of(persistenceService).getPersistence(quorumA);
      atLeast(1).of(persistenceService).getPersistence(quorumB);
    }});

    oLog.logEntry(makeSingleEntryList(1, 1, "x"), quorumA);
    oLog.logEntry(makeSingleEntryList(1, 1, "x"), quorumB);
  }

}
