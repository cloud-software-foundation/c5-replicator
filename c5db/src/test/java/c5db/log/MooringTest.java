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

import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static c5db.log.LogTestUtil.makeProtostuffEntry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class MooringTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();
  private final OLog oLog = context.mock(OLog.class);
  private final String quorumId = "quorumId";
  private ReplicatorLog log;

  @Before
  public void accessesOLogToObtainTheLastTermAndIndexWhenItIsConstructed() throws Exception {
    context.checking(new Expectations() {{
      oneOf(oLog).getLastSeqNum(quorumId);
      will(returnValue(Futures.immediateFuture(0L)));
      oneOf(oLog).getLastTerm(quorumId);
      will(returnValue(Futures.immediateFuture(0L)));
    }});

    log = new Mooring(oLog, quorumId);
  }

  @Test
  public void returnsZeroFromGetLastTermWhenLogIsEmpty() {
    ignoringLog();
    assertThat(log.getLastTerm(), is(equalTo(0L)));
  }

  @Test
  public void returnsZeroFromGetLastIndexWhenLogIsEmpty() {
    ignoringLog();
    assertThat(log.getLastIndex(), is(equalTo(0L)));
  }

  @Test(expected = Exception.class)
  public void doesNotAcceptARequestToLogAnEmptyEntryList() {
    log.logEntries(new ArrayList<>());
  }

  @Test
  public void canReturnTheTermAndIndexOfTheLastEntryLogged() {
    expectLoggingNTimes(1);

    log.logEntries(
        singleEntryList(index(12), term(34), someData()));

    assertThat(log.getLastIndex(), is(equalTo(12L)));
    assertThat(log.getLastTerm(), is(equalTo(34L)));
  }

  @Test
  public void delegatesLogAndTruncationRequestsToOLog() {
    long index = 12;

    expectLoggingNTimes(1);
    expectTruncationNTimes(1);
    oLogGetTermWillReturn(0);

    log.logEntries(
        singleEntryList(index, term(34), someData()));

    log.truncateLog(index);
  }

  @Test
  public void canReturnTheTermAndIndexOfAnEntryAfterPerformingATruncation() {
    long termOfFirstEntry = 34;
    long indexOfFirstEntry = 12;

    expectLoggingNTimes(1);
    expectTruncationNTimes(1);
    oLogGetTermWillReturn(termOfFirstEntry);

    log.logEntries(
        Lists.newArrayList(
            makeProtostuffEntry(indexOfFirstEntry, termOfFirstEntry, someData()),
            makeProtostuffEntry(index(13), term(35), someData()))
    );
    log.truncateLog(13);

    assertThat(log.getLastIndex(), is(equalTo(indexOfFirstEntry)));
    assertThat(log.getLastTerm(), is(equalTo(termOfFirstEntry)));
  }

  private long index(long i) {
    return i;
  }

  private long term(long i) {
    return i;
  }

  private String someData() {
    return "test data";
  }

  private List<LogEntry> singleEntryList(long index, long term, String stringData) {
    return Lists.newArrayList(makeProtostuffEntry(index, term, stringData));
  }

  @SuppressWarnings("unchecked")
  private void expectLoggingNTimes(int n) {
    context.checking(new Expectations() {{
      exactly(n).of(oLog).logEntry(with.is(any(List.class)), with(any(String.class)));
    }});
  }

  private void expectTruncationNTimes(int n) {
    context.checking(new Expectations() {{
      exactly(n).of(oLog).truncateLog(with(any(Long.class)), with(any(String.class)));
    }});
  }

  private void oLogGetTermWillReturn(long expectedTerm) {
    context.checking(new Expectations() {{
      exactly(1).of(oLog).getLogTerm(with(any(Long.class)), with(any(String.class)));
      will(returnValue(expectedTerm));
    }});
  }

  private void ignoringLog() {
    context.checking(new Expectations() {{
      ignoring(oLog);
    }});
  }
}
