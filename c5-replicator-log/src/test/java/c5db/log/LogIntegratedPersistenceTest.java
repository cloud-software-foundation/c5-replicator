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
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.util.CheckedConsumer;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.collect.Lists;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.file.Path;
import java.util.concurrent.Executors;

import static c5db.FutureMatchers.resultsIn;
import static c5db.log.LogMatchers.aListOfEntriesWithConsecutiveSeqNums;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.ReplicatorLogGenericTestUtil.seqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.someData;
import static c5db.log.ReplicatorLogGenericTestUtil.term;
import static c5db.replication.ReplicatorTestUtil.entries;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test the chain Mooring -> QuorumDelegatingLog -> EncodedSequentialLog -> (disk), ensuring that
 * persisted information can be reconstructed from the information left on disk, so that a replicator
 * can pick up where it left off after a crash.
 */
public class LogIntegratedPersistenceTest {
  private static final String QUORUM_ID = "LogIntegratedPersistenceTest";
  private static final int NUM_THREADS = 3;

  private final Path testDirectory = (new C5CommonTestUtil()).getDataTestDir("olog");

  @Test
  public void anEmptyLogReturnsDefaultValuesForIndexesAndTermsAndConfigurations() throws Exception {
    withReplicatorLog((log) -> {
      assertThat(log.getLastIndex(), is(equalTo(0L)));
      assertThat(log.getLastTerm(), is(equalTo(0L)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(0L)));
      assertThat(log.getLastConfiguration(), is(equalTo(QuorumConfiguration.EMPTY)));
    });
  }

  @Test
  public void termAndIndexInformationIsPersisted() throws Exception {
    withReplicatorLog((log) -> {
      log.logEntries(
          entries()
              .term(77).indexes(1, 2)
              .term(78).indexes(3, 4).build());

    });

    withReplicatorLog((log) -> {
      assertThat(log.getLastIndex(), is(equalTo(4L)));
      assertThat(log.getLastTerm(), is(equalTo(78L)));

      assertThat(log.getLogTerm(3), is(equalTo(78L)));
      assertThat(log.getLogTerm(2), is(equalTo(77L)));
    });
  }

  @Test
  public void configurationInformationIsPersisted() throws Exception {
    final QuorumConfiguration firstConfig = QuorumConfiguration
        .of(Lists.newArrayList(1L, 2L, 3L))
        .getTransitionalConfiguration(Lists.newArrayList(2L, 3L, 4L));
    final QuorumConfiguration secondConfig = firstConfig.getCompletedConfiguration();

    withReplicatorLog((log) -> {
      log.logEntries(
          entries()
              .term(97).indexes(1, 2)
              .term(98).configurationAndIndex(firstConfig, 3)
              .term(99).indexes(4, 5)
              .term(99).configurationAndIndex(secondConfig, 6).build());

    });

    withReplicatorLog((log) -> {
      assertThat(log.getLastConfiguration(), is(equalTo(secondConfig)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(6L)));

      log.truncateLog(seqNum(4));

      assertThat(log.getLastConfiguration(), is(equalTo(firstConfig)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(3L)));

      log.truncateLog(seqNum(2));

      assertThat(log.getLastConfiguration(), is(equalTo(QuorumConfiguration.EMPTY)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(0L)));
    });
  }

  @Test(expected = RuntimeException.class)
  public void informationAboutWhatSequenceNumberComesNextIsPersistedSoThatAnIncorrectSeqNumCanBeCaught()
      throws Exception {
    withReplicatorLog((log) -> {
      log.logEntries(
          entries().term(1).indexes(1, 2, 3).build());
    });

    withReplicatorLog((log) -> {
      log.logEntries(
          entries().term(2).indexes(5, 6, 7).build());
    });
  }

  @Test(timeout = 3000)
  public void headerInformationIsPersistedAfterARollOperation() throws Exception {
    final QuorumConfiguration configuration = QuorumConfiguration.of(Lists.newArrayList(1L, 2L, 3L));
    final long lastTerm = 97;

    try (OLog oLog = getOLog()) {
      new Mooring(oLog, QUORUM_ID)
          .logEntries(entries()
              .term(lastTerm)
              .configurationAndIndex(configuration, 1).build());
      oLog.roll(QUORUM_ID).get();
    }

    withReplicatorLog((log) -> {
      assertThat(log.getLastTerm(), is(equalTo(lastTerm)));
      assertThat(log.getLastConfiguration(), is(equalTo(configuration)));
    });
  }

  @Test(timeout = 3000)
  public void rollsKeepingTrackOfTermCorrectly() throws Exception {
    withOpenOLog((oLog) -> {
      oLog.logEntries(makeSingleEntryList(seqNum(1), term(17), someData()), QUORUM_ID);
      oLog.roll(QUORUM_ID);
      oLog.logEntries(makeSingleEntryList(seqNum(2), term(56), someData()), QUORUM_ID);
    });

    withOpenOLog((oLog) -> {
      assertThat(oLog.getLastTerm(QUORUM_ID), Is.is(equalTo(56L)));

      oLog.truncateLog(seqNum(2), QUORUM_ID);
      assertThat(oLog.getLastTerm(QUORUM_ID), Is.is(equalTo(17L)));
    });
  }

  @Test(timeout = 3000)
  public void performsMultiLogFetchesCorrectlyFromPersistence() throws Exception {
    withOpenOLog((oLog) -> {
      oLog.logEntries(someConsecutiveEntries(1, 6), QUORUM_ID);
      oLog.roll(QUORUM_ID);
      oLog.logEntries(someConsecutiveEntries(6, 11), QUORUM_ID);
      oLog.roll(QUORUM_ID);
      oLog.logEntries(someConsecutiveEntries(11, 16), QUORUM_ID);
    });

    withOpenOLog((oLog) ->
        assertThat(oLog.getLogEntries(3, 15, QUORUM_ID), resultsIn(aListOfEntriesWithConsecutiveSeqNums(3, 15))));

    withOpenOLog((oLog) ->
        assertThat(oLog.getLogEntries(1, 6, QUORUM_ID), resultsIn(aListOfEntriesWithConsecutiveSeqNums(1, 6))));
  }

  private void withReplicatorLog(CheckedConsumer<ReplicatorLog, Exception> useLog) throws Exception {
    try (OLog oLog = getOLog()) {
      useLog.accept(new Mooring(oLog, QUORUM_ID));
    }
  }

  private void withOpenOLog(CheckedConsumer<OLog, Exception> useLog) throws Exception {
    try (OLog oLog = getOLog()) {
      oLog.openAsync(QUORUM_ID).get();
      useLog.accept(oLog);
    }
  }

  private OLog getOLog() throws Exception {
    return new QuorumDelegatingLog(
        new LogFileService(testDirectory),
        new WrappingKeySerializingExecutor(Executors.newFixedThreadPool(NUM_THREADS)),
        NavigableMapOLogEntryOracle::new,
        InMemoryPersistenceNavigator::new);
  }

}
