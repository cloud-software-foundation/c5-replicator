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
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static c5db.FutureMatchers.resultsIn;
import static c5db.FutureMatchers.resultsInException;
import static c5db.log.LogTestUtil.aSeqNum;
import static c5db.log.LogTestUtil.emptyEntryList;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.LogTestUtil.someData;
import static c5db.log.LogTestUtil.term;
import static c5db.log.SequentialLog.LogEntryNotFound;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QuorumDelegatingLogTest {
  private static Path testDirectory;
  private LogFileService logPersistenceService;
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
    logPersistenceService = new LogFileService(testDirectory);
    logPersistenceService.moveLogsToArchive();

    log = new QuorumDelegatingLog(
        logPersistenceService,
        new WrappingKeySerializingExecutor(MoreExecutors.sameThreadExecutor()),
        NavigableMapOLogEntryOracle::new,
        InMemoryPersistenceNavigator::new);

    log.openAsync(quorumId).get();
  }

  @After
  public final void tearDown() throws Exception {
    log.close();
    logPersistenceService.moveLogsToArchive();
  }

  @Test(timeout = 1000)
  public void throwsExceptionFromGetLogEntriesMethodWhenTheLogIsEmpty() throws Exception {
    assertThat(log.getLogEntries(1, 2, quorumId), resultsInException(LogEntryNotFound.class));
  }

  @Test
  public void retrievesListsOfEntriesItHasLogged() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(1, 5);

    log.logEntry(entries, quorumId);

    assertThat(log.getLogEntries(1, 5, quorumId),
        resultsIn(equalTo(entries)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsAnExceptionWhenGettingEntriesIfEndIsLessThanStart() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.getLogEntries(3, 2, quorumId);
  }

  @Test
  public void returnsAnEmptyListWhenGettingEntriesIfStartEqualsEnd() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
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

    log.logEntry(entriesA, quorumA);
    log.logEntry(entriesB, quorumB);

    assertThat(log.getLogEntries(1, 5, quorumA), resultsIn(equalTo(entriesA)));
    assertThat(log.getLogEntries(1, 5, quorumB), resultsIn(equalTo(entriesB)));
  }

  @Test
  public void retrievesEntriesFromTheMiddleOfTheLog() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(1, 10);

    log.logEntry(entries, quorumId);

    assertThat(log.getLogEntries(4, 6, quorumId), resultsIn(equalTo(entries.subList(3, 5))));
  }

  @Test
  public void truncatesEntriesFromTheEndOfTheLog() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.logEntry(someConsecutiveEntries(3, 5), quorumId);
  }

  @Test(expected = RuntimeException.class)
  public void throwsAnExceptionIfAttemptingToLogEntriesWithASequenceGap() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 3), quorumId);
    log.logEntry(someConsecutiveEntries(4, 5), quorumId);
  }

  @Test(expected = RuntimeException.class)
  public void throwsAnExceptionIfAttemptingToLogEntriesWithoutAscendingSequenceNumber() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 2), quorumId);
    log.logEntry(someConsecutiveEntries(1, 2), quorumId);
  }

  @Test(timeout = 1000)
  public void throwsAnExceptionIfTryingToRetrieveEntriesAndAtLeastOneIsNotInTheLog() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);

    assertThat(log.getLogEntries(2, 4, quorumId), resultsInException(LogEntryNotFound.class));
  }

  @Test
  public void maintainsCorrectSequenceAfterATruncation() {
    log.logEntry(someConsecutiveEntries(1, 5), quorumId);
    log.truncateLog(3, quorumId);
    log.logEntry(someConsecutiveEntries(3, 6), quorumId);
  }

  @Test
  public void storesAndRetrievesElectionTermForEntriesItHasLogged() {
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(1), someData()), quorumId);
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(2), someData()), quorumId);

    assertThat(log.getLogTerm(lastSeqNum(), quorumId), is(equalTo(term(2))));
    assertThat(log.getLogTerm(lastSeqNum() - 1, quorumId), is(equalTo(term(1))));
  }

  @Test
  public void retrievesCorrectElectionTermAfterATruncation() {
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(1), someData()), quorumId);
    log.logEntry(makeSingleEntryList(nextSeqNum(), term(2), someData()), quorumId);
    log.truncateLog(lastSeqNum(), quorumId);
    log.logEntry(makeSingleEntryList(lastSeqNum(), term(3), someData()), quorumId);

    assertThat(log.getLogTerm(lastSeqNum(), quorumId), is(equalTo(term(3))));
  }

  @Test
  public void returnsResultsFromGetLogEntriesThatTakeIntoAccountMutationsRequestedEarlier() throws Exception {
    log.logEntry(someConsecutiveEntries(1, 10), quorumId);
    log.truncateLog(5, quorumId);
    List<OLogEntry> replacementEntries = someConsecutiveEntries(5, 10);
    log.logEntry(replacementEntries, quorumId);

    assertThat(log.getLogEntries(5, 10, quorumId), resultsIn(equalTo(replacementEntries)));
  }


  /**
   * Private methods
   */

  private long testSequenceNumber = aSeqNum();

  private long nextSeqNum() {
    testSequenceNumber++;
    return testSequenceNumber;
  }

  private long lastSeqNum() {
    return testSequenceNumber;
  }

}
