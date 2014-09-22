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
import c5db.MiscMatchers;
import c5db.generated.OLogHeader;
import c5db.interfaces.replication.QuorumConfiguration;
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
import java.util.List;

import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static c5db.log.LogMatchers.equalToHeader;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.ReplicatorLogGenericTestUtil.term;
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
    final long anArbitrarySeqNum = 1210;
    final long aGreaterArbitrarySeqNum = 1815;

    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(anArbitrarySeqNum));
    havingAppendedAPersistenceContainingHeader(secondHeader = anOLogHeaderWithSeqNum(aGreaterArbitrarySeqNum));

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
    final long anArbitrarySeqNum = 1210;
    final long aGreaterArbitrarySeqNum = 1815;

    havingAppendedAPersistenceContainingHeader(firstHeader = anOLogHeaderWithSeqNum(anArbitrarySeqNum));
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(aGreaterArbitrarySeqNum));

    logFileService.truncate(QUORUM_ID);

    try (FilePersistence persistence = logFileService.getCurrent(QUORUM_ID)) {
      assertThat(deserializedHeader(persistence), is(equalToHeader(firstHeader)));
    }
  }

  @Test
  public void listsDataStoresInOrderOfMostRecentToLeastRecent() throws Exception {
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(1));
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(2));
    havingAppendedAPersistenceContainingHeader(anOLogHeaderWithSeqNum(3));

    assertThat(logFileService.getList(QUORUM_ID), is(aListOfPersistencesWithSeqNums(3, 2, 1)));
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
    try (InputStream input = Channels.newInputStream(persistence.getReader())) {
      return decodeAndCheckCrc(input, OLogHeader.getSchema());
    }
  }

  private static QuorumConfigurationMessage configurationOf(long... peerIds) {
    return QuorumConfiguration.of(Longs.asList(peerIds)).toProtostuff();
  }

  private static Matcher<List<? extends CheckedSupplier<? extends BytePersistence, IOException>>> aListOfPersistencesWithSeqNums(
      long... seqNumsInHeaders) {
    return MiscMatchers.simpleMatcherForPredicate((list) -> {
      try {
        int seqNumIndex = 0;

        for (CheckedSupplier<? extends BytePersistence, IOException> persistenceSupplier : list) {
          BytePersistence persistence = persistenceSupplier.get();
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
    }, (description) -> description.appendText("a List of BytePersistence Suppliers, where the persistence" +
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
