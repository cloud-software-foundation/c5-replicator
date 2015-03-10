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
import c5db.FutureMatchers;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static c5db.FutureMatchers.resultsIn;
import static c5db.log.LogMatchers.aListOfEntriesWithConsecutiveSeqNums;
import static c5db.log.LogTestUtil.emptyEntryList;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.seqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.someData;
import static c5db.log.ReplicatorLogGenericTestUtil.term;
import static c5db.log.SequentialLog.LogEntryNotFound;
import static c5db.replication.ReplicatorTestUtil.makeConfigurationEntry;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QuorumDelegatingLogTest {
  private static Path testDirectory;
  private LogFileService logFileService;
  private OLog log;
  private final String quorumId = "quorumId";

  @BeforeClass
  public static void setTestDirectory() {
    if (testDirectory == null) {
      testDirectory = (new C5CommonTestUtil()).getDataTestDir("olog");
    }
  }

  @Before
  public final void setUp() throws Exception {
    logFileService = new LogFileService(testDirectory);
    logFileService.clearAllLogs();

    log = new QuorumDelegatingLog(
        logFileService,
        new WrappingKeySerializingExecutor(MoreExecutors.sameThreadExecutor()),
        new OLogEntryOracle.OLogEntryOracleFactory() {
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

    log.openAsync(quorumId).get();
  }

  @After
  public final void tearDown() throws Exception {
    log.close();
    logFileService.clearAllLogs();
  }

  @Test(timeout = 1000)
  public void throwsExceptionFromGetLogEntriesMethodWhenTheLogIsEmpty() throws Exception {
    assertThat(log.getLogEntries(1, 2, quorumId), FutureMatchers.<List<OLogEntry>>resultsInException(LogEntryNotFound.class));
  }

  @Test
  public void retrievesListsOfEntriesItHasLogged() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(1, 5);

    log.logEntries(entries, quorumId);

    assertThat(log.getLogEntries(1, 5, quorumId),
        resultsIn(equalTo(entries)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsAnExceptionWhenGettingEntriesIfEndIsLessThanStart() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 5), quorumId);
    log.getLogEntries(3, 2, quorumId);
  }

  @Test
  public void returnsAnEmptyListWhenGettingEntriesIfStartEqualsEnd() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 5), quorumId);
    assertThat(log.getLogEntries(3, 3, quorumId), resultsIn(equalTo(emptyEntryList())));
  }

  @Test
  public void logsAndRetrievesDifferentQuorumsWithTheSameSequenceNumbers() throws Exception {
    String quorumA = "A";
    String quorumB = "B";

    log.openAsync(quorumA);
    log.openAsync(quorumB);

    List<OLogEntry> entriesA = someConsecutiveEntries(1, 5);
    List<OLogEntry> entriesB = someConsecutiveEntries(1, 5);

    log.logEntries(entriesA, quorumA);
    log.logEntries(entriesB, quorumB);

    assertThat(log.getLogEntries(1, 5, quorumA), resultsIn(equalTo(entriesA)));
    assertThat(log.getLogEntries(1, 5, quorumB), resultsIn(equalTo(entriesB)));
  }

  @Test
  public void retrievesEntriesFromTheMiddleOfTheLog() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(1, 10);

    log.logEntries(entries, quorumId);

    assertThat(log.getLogEntries(4, 6, quorumId), resultsIn(equalTo(subListWithSeqNums(entries, 4, 6))));
  }

  @Test
  public void truncatesEntriesFromTheEndOfTheLogAndMaintainsTheCorrectSequence() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.logEntries(someConsecutiveEntries(3, 5), quorumId);

    assertThat(log.getLogEntries(1, 5, quorumId), resultsIn(aListOfEntriesWithConsecutiveSeqNums(1, 5)));
  }

  @Test(expected = RuntimeException.class)
  public void throwsAnExceptionIfAskedToLogEntriesWithASequenceGap() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 3), quorumId);
    log.logEntries(someConsecutiveEntries(4, 5), quorumId);
  }

  @Test(expected = RuntimeException.class)
  public void throwsAnExceptionIfAskedToLogEntriesWithoutAscendingSequenceNumber() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 2), quorumId);
    log.logEntries(someConsecutiveEntries(1, 2), quorumId);
  }

  @Test(timeout = 1000)
  public void returnsAFutureWithAnExceptionIfAskedToRetrieveEntriesAndAtLeastOneIsNotInTheLog() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);

    assertThat(log.getLogEntries(2, 4, quorumId), FutureMatchers.<List<OLogEntry>>resultsInException(LogEntryNotFound.class));
  }

  @Test
  public void returnsTheExpectedNextSequenceNumber() {
    assertThat(log.getNextSeqNum(quorumId), equalTo(1L));

    log.logEntries(someConsecutiveEntries(1, 4), quorumId);
    assertThat(log.getNextSeqNum(quorumId), equalTo(4L));

    log.truncateLog(seqNum(2), quorumId);
    assertThat(log.getNextSeqNum(quorumId), equalTo(2L));
  }

  @Test
  public void storesAndRetrievesElectionTermForEntriesItHasLogged() {
    log.logEntries(makeSingleEntryList(nextSeqNum(), term(1), someData()), quorumId);
    log.logEntries(makeSingleEntryList(nextSeqNum(), term(2), someData()), quorumId);

    assertThat(log.getLastTerm(quorumId), is(equalTo(term(2))));
    assertThat(log.getLogTerm(lastSeqNum(), quorumId), is(equalTo(term(2))));
    assertThat(log.getLogTerm(lastSeqNum() - 1, quorumId), is(equalTo(term(1))));
  }

  @Test
  public void retrievesCorrectElectionTermAfterATruncation() {
    log.logEntries(makeSingleEntryList(nextSeqNum(), term(1), someData()), quorumId);
    log.logEntries(makeSingleEntryList(nextSeqNum(), term(2), someData()), quorumId);
    log.truncateLog(lastSeqNum(), quorumId);
    log.logEntries(makeSingleEntryList(lastSeqNum(), term(3), someData()), quorumId);

    assertThat(log.getLastTerm(quorumId), is(equalTo(term(3))));
    assertThat(log.getLogTerm(lastSeqNum(), quorumId), is(equalTo(term(3))));
  }

  @Test
  public void retrievesTheLastQuorumConfigurationAndItsSequenceNumber() {
    QuorumConfiguration firstConfig = QuorumConfiguration.of(Sets.newHashSet(1L, 2L, 3L));
    QuorumConfiguration secondConfig = firstConfig.getTransitionalConfiguration(Sets.newHashSet(4L, 5L, 6L));

    log.logEntries(singleConfigurationEntryList(firstConfig, seqNum(1)), quorumId);
    assertThat(log.getLastQuorumConfig(quorumId),
        equalTo(new QuorumConfigurationWithSeqNum(firstConfig, seqNum(1))));

    log.logEntries(singleConfigurationEntryList(secondConfig, seqNum(2)), quorumId);
    assertThat(log.getLastQuorumConfig(quorumId),
        equalTo(new QuorumConfigurationWithSeqNum(secondConfig, seqNum(2))));

    log.truncateLog(seqNum(2), quorumId);
    assertThat(log.getLastQuorumConfig(quorumId),
        equalTo(new QuorumConfigurationWithSeqNum(firstConfig, seqNum(1))));
  }

  @Test
  public void returnsResultsFromGetLogEntriesThatTakeIntoAccountMutationsRequestedEarlier() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 10), quorumId);
    log.truncateLog(5, quorumId);
    List<OLogEntry> replacementEntries = someConsecutiveEntries(5, 10);
    log.logEntries(replacementEntries, quorumId);

    assertThat(log.getLogEntries(5, 10, quorumId), resultsIn(equalTo(replacementEntries)));
  }

  @Test(timeout = 3000)
  public void rollsTheLogWhileAcceptingLogRequestsAndEnsuresThatAllRequestedEntriesEndUpInTheNewLog() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 11), quorumId);
    log.roll(quorumId);
    log.logEntries(someConsecutiveEntries(11, 21), quorumId);

    assertThat(log.getLogEntries(11, 21, quorumId), resultsIn(aListOfEntriesWithConsecutiveSeqNums(11, 21)));
  }

  @Test
  public void truncatesARolledFileIfRequestedToTruncateToAPointBeforeTheBeginningOfTheCurrentFile() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 11), quorumId);
    log.roll(quorumId);
    log.truncateLog(seqNum(6), quorumId);
    log.logEntries(someConsecutiveEntries(6, 21), quorumId);

    assertThat(log.getLogEntries(6, 21, quorumId), resultsIn(aListOfEntriesWithConsecutiveSeqNums(6, 21)));
  }

  @Test
  public void fulfillsGetRequestsThatSpanMultipleLogFiles() throws Exception {
    log.logEntries(someConsecutiveEntries(1, 6), quorumId);
    log.roll(quorumId);
    log.logEntries(someConsecutiveEntries(6, 11), quorumId);
    log.roll(quorumId);
    log.logEntries(someConsecutiveEntries(11, 16), quorumId);

    assertThat(log.getLogEntries(3, 15, quorumId), resultsIn(aListOfEntriesWithConsecutiveSeqNums(3, 15)));
  }

  /**
   * Private methods
   */

  private long testSequenceNumber = 0;

  private long nextSeqNum() {
    testSequenceNumber++;
    return testSequenceNumber;
  }

  private long lastSeqNum() {
    return testSequenceNumber;
  }

  private static List<OLogEntry> singleConfigurationEntryList(QuorumConfiguration config, long seqNum) {
    return Lists.newArrayList(
        OLogEntry.fromProtostuff(
            makeConfigurationEntry(seqNum, term(1), config)));
  }

  private static List<OLogEntry> subListWithSeqNums(List<OLogEntry> entryList, long start, long end) {
    List<OLogEntry> result = new ArrayList<>();
    for (OLogEntry entry : entryList) {
      if (start <= entry.getSeqNum() && entry.getSeqNum() < end) {
        result.add(entry);
      }
    }
    return result;
  }
}
