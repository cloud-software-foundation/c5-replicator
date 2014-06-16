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
import c5db.MiscMatchers;
import c5db.generated.OLogHeader;
import c5db.replication.QuorumConfiguration;
import c5db.replication.generated.QuorumConfigurationMessage;
import c5db.util.CheckedSupplier;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static c5db.log.LogMatchers.equalToHeader;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LogFileServiceTest {
  private static final String QUORUM_ID = "q";
  private final Path testDirectory = (new C5CommonTestUtil()).getDataTestDir("log-file-service-test");

  private LogFileService logFileService;

  @Before
  public void createTestObject() throws Exception {
    logFileService = new LogFileService(testDirectory);
  }

  @Test(expected = IOException.class)
  public void throwsAnIOExceptionIfAttemptingToAppendToTheFileAfterClosingIt() throws Exception {
    try (FilePersistence persistence = logFileService.create(QUORUM_ID)) {
      persistence.close();
      persistence.append(serializedHeader(anOLogHeader()));
    }
  }

  @Test
  public void returnsNullWhenCallingGetCurrentWhenThereAreNoLogs() throws Exception {
    assertThat(logFileService.getCurrent(QUORUM_ID), nullValue());
  }

  @Test
  public void returnsTheLatestAppendedPersistenceForAGivenQuorum() throws Exception {
    final OLogHeader header = anOLogHeader();
    havingAppendedAPersistenceContainingHeader(header);

    try (BytePersistence persistence = logFileService.getCurrent(QUORUM_ID)) {
      assertThat(deserializedHeader(persistence), is(equalToHeader(header)));
    }
  }

  @Test
  public void returnsANewEmptyPersistenceOnEachCallToCreate() throws Exception {
    havingAppendedAPersistenceContainingHeader(anOLogHeader());

    try (BytePersistence aSecondPersistence = logFileService.create(QUORUM_ID)) {
      assertThat(aSecondPersistence, isEmpty());
    }
  }

  @Test
  public void appendsANewPersistenceSoThatItWillBeReturnedByFutureCallsToGetCurrent() throws Exception {
    final OLogHeader secondHeader;

    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(1210));
    havingAppendedAPersistenceContainingHeader(secondHeader = anOLogHeaderWithSeqNum(1815));

    try (BytePersistence primaryPersistence = logFileService.getCurrent(QUORUM_ID)) {
      assertThat(deserializedHeader(primaryPersistence), is(equalToHeader(secondHeader)));
    }
  }

  @Test
  public void anExistingPersistenceRemainsUsableAfterBeingAppended() throws Exception {
    final OLogHeader header = anOLogHeader();

    try (FilePersistence persistence = logFileService.create(QUORUM_ID)) {
      persistence.append(serializedHeader(header));
      logFileService.append(QUORUM_ID, persistence);

      assertThat(deserializedHeader(persistence), is(equalToHeader(header)));
    }
  }

  @Test
  public void removesTheMostRecentAppendedDataStoreWhenTruncateIsCalled() throws Exception {
    final OLogHeader firstHeader;

    havingAppendedAPersistenceContainingHeader(firstHeader = anOLogHeaderWithSeqNum(1210));
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(1815));

    logFileService.truncate(QUORUM_ID);

    try (FilePersistence persistence = logFileService.getCurrent(QUORUM_ID)) {
      assertThat(deserializedHeader(persistence), is(equalToHeader(firstHeader)));
    }
  }

  @Test
  public void iteratesOverDataStoresInOrderOfMostRecentToLeastRecent() throws Exception {
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(1));
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(2));
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(3));

    assertThat(logFileService.iterator(QUORUM_ID), is(anIteratorOverPersistencesWithSeqNums(3, 2, 1)));
  }


  private void havingAppendedAPersistenceContainingHeader(OLogHeader header) throws Exception {
    try (FilePersistence persistenceToReplacePrimary = logFileService.create(QUORUM_ID)) {
      persistenceToReplacePrimary.append(serializedHeader(header));
      logFileService.append(QUORUM_ID, persistenceToReplacePrimary);
    }
  }

  private static OLogHeader anOLogHeader() {
    return anOLogHeaderWithSeqNum(1);
  }

  private static OLogHeader anOLogHeaderWithSeqNum(long seqNum) {
    return new OLogHeader(term(1), seqNum, configurationOf(1, 2, 3));
  }

  private ByteBuffer[] serializedHeader(OLogHeader header) {
    List<ByteBuffer> buffers = encodeWithLengthAndCrc(OLogHeader.getSchema(), header);
    return Iterables.toArray(buffers, ByteBuffer.class);
  }

  private static OLogHeader deserializedHeader(BytePersistence persistence) throws Exception {
    InputStream input = Channels.newInputStream(persistence.getReader());
    return decodeAndCheckCrc(input, OLogHeader.getSchema());
  }

  private static QuorumConfigurationMessage configurationOf(long... peerIds) {
    return QuorumConfiguration.of(Longs.asList(peerIds)).toProtostuff();
  }

  private static Matcher<Iterator<? extends CheckedSupplier<? extends BytePersistence, IOException>>> anIteratorOverPersistencesWithSeqNums(
      long... seqNumsInHeaders) {
    return MiscMatchers.simpleMatcherForPredicate((iterator) -> {
      try {
        int seqNumIndex = 0;

        while (iterator.hasNext()) {
          BytePersistence persistence = iterator.next().get();
          OLogHeader header = deserializedHeader(persistence);
          if (header.getBaseSeqNum() != seqNumsInHeaders[seqNumIndex]) {
            return false;
          }
          seqNumIndex++;
        }

        return seqNumIndex == seqNumsInHeaders.length;
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }, (description) -> description.appendText("an Iterator over BytePersistence Suppliers, where the persistence" +
        " objects begin with headers with sequence numbers ").appendValue(seqNumsInHeaders).appendText(" in order"));
  }

  private static Matcher<BytePersistence> isEmpty() {
    return MiscMatchers.simpleMatcherForPredicate((persistence) -> {
      try {
        return persistence.isEmpty();
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }, (description) -> description.appendText("is empty"));
  }
}
