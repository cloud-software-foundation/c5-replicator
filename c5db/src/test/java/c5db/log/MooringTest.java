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

import c5db.replication.QuorumConfiguration;
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

import static c5db.log.LogTestUtil.makeConfigurationEntry;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.makeProtostuffEntry;
import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;
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
      oneOf(oLog).openAsync(quorumId);
      will(returnValue(
          Futures.immediateFuture(makeEntry(0, 0, ""))));

      oneOf(oLog).getQuorumConfig(0, quorumId);
      will(returnValue(zeroConfiguration()));
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

    context.checking(new Expectations() {{
      oneOf(oLog).getQuorumConfig(index - 1, quorumId);
      will(returnValue(zeroConfiguration()));
    }});

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

    context.checking(new Expectations() {{
      oneOf(oLog).getQuorumConfig(indexOfFirstEntry, quorumId);
      will(returnValue(zeroConfiguration()));
    }});

    log.logEntries(
        Lists.newArrayList(
            makeProtostuffEntry(indexOfFirstEntry, termOfFirstEntry, someData()),
            makeProtostuffEntry(indexOfFirstEntry + 1, term(35), someData())));
    log.truncateLog(indexOfFirstEntry + 1);

    assertThat(log.getLastIndex(), is(equalTo(indexOfFirstEntry)));
    assertThat(log.getLastTerm(), is(equalTo(termOfFirstEntry)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsAnExceptionIfAskedToTruncateToAnIndexOfZero() {
    log.truncateLog(0);
  }

  @Test
  public void storesAndRetrievesTheLastQuorumConfigurationLogged() {
    final long term = 7;
    final QuorumConfiguration config = aQuorumConfiguration();

    expectLoggingNTimes(1);

    log.logEntries(
        Lists.newArrayList(
            makeProtostuffEntry(index(2), term, someData()),
            makeConfigurationEntry(index(3), term, config.toProtostuff()),
            makeProtostuffEntry(index(4), term, someData())
        ));

    assertThat(log.getLastConfiguration(), is(equalTo(config)));
    assertThat(log.getLastConfigurationIndex(), is(equalTo(3L)));
  }

  @Test
  public void retrievesTheEmptyQuorumConfigurationWhenTheLogIsEmpty() {
    assertThat(log.getLastConfiguration(), is(equalTo(QuorumConfiguration.EMPTY)));
    assertThat(log.getLastConfigurationIndex(), is(equalTo(0L)));
  }

  @Test
  public void retrievesAnEarlierQuorumConfigurationWhenALaterOneIsTruncated() {
    final long term = 7;
    final QuorumConfiguration firstConfig = aQuorumConfiguration();
    final long firstConfigSeqNum = 777;
    final QuorumConfiguration secondConfig = firstConfig.completeTransition();
    final long secondConfigSeqNum = firstConfigSeqNum + 1;

    expectLoggingNTimes(1);
    expectTruncationNTimes(1);
    allowOLogGetTerm();

    log.logEntries(
        Lists.newArrayList(
            makeConfigurationEntry(firstConfigSeqNum, term, firstConfig.toProtostuff()),
            makeConfigurationEntry(secondConfigSeqNum, term, secondConfig.toProtostuff())
        ));

    assertThat(log.getLastConfiguration(), is(equalTo(secondConfig)));
    assertThat(log.getLastConfigurationIndex(), is(equalTo(secondConfigSeqNum)));

    context.checking(new Expectations() {{
      oneOf(oLog).getQuorumConfig(firstConfigSeqNum, quorumId);
      will(returnValue(new QuorumConfigurationWithSeqNum(firstConfig, firstConfigSeqNum)));
    }});

    log.truncateLog(secondConfigSeqNum);

    assertThat(log.getLastConfiguration(), is(equalTo(firstConfig)));
    assertThat(log.getLastConfigurationIndex(), is(equalTo(firstConfigSeqNum)));
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

  private QuorumConfiguration aQuorumConfiguration() {
    return QuorumConfiguration
        .of(Lists.newArrayList(1L, 2L, 3L))
        .transitionTo(Lists.newArrayList(4L, 5L, 6L));
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

  private void allowOLogGetTerm() {
    context.checking(new Expectations() {{
      allowing(oLog).getLogTerm(with(any(Long.class)), with(any(String.class)));
    }});
  }

  private void ignoringLog() {
    context.checking(new Expectations() {{
      ignoring(oLog);
    }});
  }

  private QuorumConfigurationWithSeqNum zeroConfiguration() {
    return new QuorumConfigurationWithSeqNum(QuorumConfiguration.EMPTY, 0);
  }
}
