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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static c5db.log.EncodedSequentialLog.Codec;
import static c5db.log.EncodedSequentialLog.LogEntryNotInSequence;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;

/**
 * OLog that delegates each quorum's logging tasks to a separate SequentialLog for that quorum,
 * executing the tasks on a KeySerializingExecutor.
 */
public class QuorumDelegatingLog implements OLog, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(QuorumDelegatingLog.class);

  private final LogPersistenceService persistenceService;
  private final KeySerializingExecutor taskExecutor;
  private final Map<String, PerQuorum> quorumMap = new HashMap<>();

  private final Supplier<TermOracle> termOracleFactory;
  private final BiFunction<BytePersistence, Codec, PersistenceNavigator> persistenceNavigatorFactory;

  public QuorumDelegatingLog(LogPersistenceService persistenceService,
                             KeySerializingExecutor taskExecutor,
                             Supplier<TermOracle> termOracleFactory,
                             BiFunction<BytePersistence, Codec, PersistenceNavigator> persistenceNavigatorFactory
  ) {
    this.persistenceService = persistenceService;
    this.taskExecutor = taskExecutor;
    this.termOracleFactory = termOracleFactory;
    this.persistenceNavigatorFactory = persistenceNavigatorFactory;
  }

  private class PerQuorum {
    public final SequentialLog<OLogEntry> quorumLog;
    public final TermOracle termOracle;
    public final Codec<OLogEntry> entryCodec = new OLogEntry.Codec();

    private long expectedNextSequenceNumber;

    public PerQuorum(String quorumId) {
      try {
        BytePersistence persistence = persistenceService.getPersistence(quorumId);
        quorumLog = new EncodedSequentialLog<>(
            persistence,
            entryCodec,
            persistenceNavigatorFactory.apply(persistence, entryCodec));
        termOracle = termOracleFactory.get();
      } catch (IOException e) {
        LOG.error("Unable to create quorum info object for quorum {}", quorumId);
        throw new RuntimeException(e);
      }
    }

    public void validateConsecutiveEntries(List<OLogEntry> entries) {
      for (OLogEntry e : entries) {
        long seqNum = e.getSeqNum();
        if (expectedNextSequenceNumber == 0 || (seqNum == expectedNextSequenceNumber)) {
          expectedNextSequenceNumber = seqNum + 1;
        } else {
          throw new LogEntryNotInSequence();
        }
      }
    }

    public void setExpectedNextSequenceNumber(long seqNum) {
      this.expectedNextSequenceNumber = seqNum;
    }
  }

  @Override
  public ListenableFuture<Boolean> logEntry(List<OLogEntry> passedInEntries, String quorumId) {
    List<OLogEntry> entries = validateAndMakeDefensiveCopy(passedInEntries);

    getOrCreateQuorumStructure(quorumId).validateConsecutiveEntries(entries);
    updateTermInformationWithNewEntries(entries, quorumId);

    // TODO group commit / sync

    return taskExecutor.submit(quorumId, () -> {
      quorumLog(quorumId).append(entries);
      return true;
    });
  }

  @Override
  public ListenableFuture<OLogEntry> getLogEntry(long seqNum, String quorumId) {
    return taskExecutor.submit(quorumId, () -> {
      List<OLogEntry> results = quorumLog(quorumId).subSequence(seqNum, seqNum + 1);
      if (results.isEmpty()) {
        return null;
      } else {
        return results.get(0);
      }
    });
  }

  @Override
  public ListenableFuture<List<OLogEntry>> getLogEntries(long start, long end, String quorumId) {
    if (end < start) {
      throw new IllegalArgumentException("getLogEntries: end < start");
    } else if (end == start) {
      return Futures.immediateFuture(Lists.newArrayList());
    }

    return taskExecutor.submit(quorumId, () -> quorumLog(quorumId).subSequence(start, end));
  }

  @Override
  public ListenableFuture<Boolean> truncateLog(long seqNum, String quorumId) {
    getOrCreateQuorumStructure(quorumId).setExpectedNextSequenceNumber(seqNum);
    termOracle(quorumId).notifyTruncation(seqNum);
    return taskExecutor.submit(quorumId, () -> {
      quorumLog(quorumId).truncate(seqNum);
      return true;
    });
  }

  @Override
  public long getLogTerm(long seqNum, String quorumId) {
    return termOracle(quorumId).getTermAtSeqNum(seqNum);
  }

  @Override
  public ListenableFuture<Long> getLastSeqNum(String quorumId) {
    return taskExecutor.submit(quorumId, () -> {
      return quorumLog(quorumId).getLastEntry().getSeqNum();
    });
  }

  @Override
  public ListenableFuture<Long> getLastTerm(String quorumId) {
    return taskExecutor.submit(quorumId, () -> {
      return quorumLog(quorumId).getLastEntry().getElectionTerm();
    });
  }

  @Override
  public void roll() throws IOException, ExecutionException, InterruptedException {
    // TODO implement roll()
  }

  @Override
  public void close() throws IOException {
    taskExecutor.shutdown();
    for (PerQuorum quorum : quorumMap.values()) {
      quorum.quorumLog.close();
    }
  }

  private List<OLogEntry> validateAndMakeDefensiveCopy(List<OLogEntry> entries) {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("Attempting to log an empty entry list");
    }

    return ImmutableList.copyOf(entries);
  }

  private PerQuorum getOrCreateQuorumStructure(String quorumId) {
    if (quorumMap.containsKey(quorumId)) {
      return quorumMap.get(quorumId);
    } else {
      PerQuorum newQuorumMap = new PerQuorum(quorumId);
      quorumMap.put(quorumId, newQuorumMap);
      return newQuorumMap;
    }
  }

  private SequentialLog<OLogEntry> quorumLog(String quorumId) {
    return getOrCreateQuorumStructure(quorumId).quorumLog;
  }

  private TermOracle termOracle(String quorumId) {
    return getOrCreateQuorumStructure(quorumId).termOracle;
  }

  private void updateTermInformationWithNewEntries(List<OLogEntry> entries, String quorumId) {
    TermOracle termOracle = termOracle(quorumId);
    for (SequentialEntry e : entries) {
      termOracle.notifyLogging(e.getSeqNum(), e.getElectionTerm());
    }
  }
}
