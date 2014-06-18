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

import c5db.generated.OLogHeader;
import c5db.replication.QuorumConfiguration;
import c5db.util.KeySerializingExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.CountingInputStream;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceNavigatorFactory;
import static c5db.log.OLogEntryOracle.OLogEntryOracleFactory;
import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;

/**
 * OLog that delegates each quorum's logging tasks to a separate SequentialLog for that quorum,
 * executing the tasks on a KeySerializingExecutor, with quorumId as the key. It is safe for use
 * by multiple threads, but each quorum's sequence numbers must be ascending with no gaps within
 * that quorum; so having multiple unsynchronized threads writing for the same quorum is unlikely
 * to work.
 */
public class QuorumDelegatingLog implements OLog, AutoCloseable {
  private final LogPersistenceService persistenceService;
  private final KeySerializingExecutor taskExecutor;
  private final Map<String, PerQuorum> quorumMap = new ConcurrentHashMap<>();

  private final OLogEntryOracleFactory OLogEntryOracleFactory;
  private final PersistenceNavigatorFactory persistenceNavigatorFactory;

  public QuorumDelegatingLog(LogPersistenceService persistenceService,
                             KeySerializingExecutor taskExecutor,
                             OLogEntryOracleFactory OLogEntryOracleFactory,
                             PersistenceNavigatorFactory persistenceNavigatorFactory
  ) {
    this.persistenceService = persistenceService;
    this.taskExecutor = taskExecutor;
    this.OLogEntryOracleFactory = OLogEntryOracleFactory;
    this.persistenceNavigatorFactory = persistenceNavigatorFactory;
  }

  private class PerQuorum {
    public final SequentialLog<OLogEntry> quorumLog;
    public final OLogEntryOracle oLogEntryOracle = OLogEntryOracleFactory.create();
    public volatile boolean opened;

    private final SequentialEntryCodec<OLogEntry> entryCodec = new OLogEntry.Codec();
    private final BytePersistence persistence;
    private final PersistenceNavigator navigator;
    private final OLogHeader header;
    private final long headerSize;

    private long expectedNextSequenceNumber;

    public PerQuorum(String quorumId) throws IOException {
      persistence = createOrOpenBytePersistence(quorumId);

      if (persistence.isEmpty()) {
        header = writeAndReturnEmptyLogHeader();
        headerSize = persistence.size();
      } else {
        CountingInputStream input = getCountingInputStream();
        header = decodeAndCheckCrc(input, OLogHeader.getSchema());
        headerSize = input.getCount();
      }

      navigator = persistenceNavigatorFactory.create(persistence, entryCodec, headerSize);
      quorumLog = new EncodedSequentialLog<>(persistence, entryCodec, navigator);
      prepareInMemoryObjects();
      opened = true;
    }

    public void validateConsecutiveEntries(List<OLogEntry> entries) {
      for (OLogEntry e : entries) {
        long seqNum = e.getSeqNum();
        if (seqNum != expectedNextSequenceNumber) {
          throw new IllegalArgumentException("Unexpected sequence number in entries requested to be logged");
        }
        expectedNextSequenceNumber++;
      }
    }

    public void setExpectedNextSequenceNumber(long seqNum) {
      this.expectedNextSequenceNumber = seqNum;
    }

    private BytePersistence createOrOpenBytePersistence(String quorumId) throws IOException {
      return persistenceService.getCurrent(quorumId);
    }

    private OLogHeader writeAndReturnEmptyLogHeader() throws IOException {
      OLogHeader header = new OLogHeader(0, 0, QuorumConfiguration.EMPTY.toProtostuff());
      List<ByteBuffer> buffers = encodeWithLengthAndCrc(OLogHeader.getSchema(), header);
      persistence.append(Iterables.toArray(buffers, ByteBuffer.class));
      return header;
    }

    private CountingInputStream getCountingInputStream() throws IOException {
      return new CountingInputStream(Channels.newInputStream(persistence.getReader()));
    }

    private void prepareInMemoryObjects() throws IOException {
      setExpectedNextSequenceNumber(header.getBaseSeqNum() + 1);
      navigator.addToIndex(header.getBaseSeqNum() + 1, headerSize);
      quorumLog.forEach((entry) -> {
        oLogEntryOracle.notifyLogging(entry);
        setExpectedNextSequenceNumber(entry.getSeqNum() + 1);
      });
    }
  }

  @Override
  public ListenableFuture<Void> openAsync(String quorumId) {
    return taskExecutor.submit(quorumId, () -> {
      createAndPrepareQuorumStructures(quorumId);
      return null;
    });
  }

  @Override
  public ListenableFuture<Boolean> logEntry(List<OLogEntry> passedInEntries, String quorumId) {
    List<OLogEntry> entries = validateAndMakeDefensiveCopy(passedInEntries);

    getQuorumStructure(quorumId).validateConsecutiveEntries(entries);
    updateOracleWithNewEntries(entries, quorumId);

    // TODO group commit / sync

    return taskExecutor.submit(quorumId, () -> {
      quorumLog(quorumId).append(entries);
      return true;
    });
  }

  @Override
  public ListenableFuture<List<OLogEntry>> getLogEntries(long start, long end, String quorumId) {
    if (end < start) {
      throw new IllegalArgumentException("getLogEntries: end < start");
    } else if (end == start) {
      return Futures.immediateFuture(new ArrayList<>());
    }

    return taskExecutor.submit(quorumId, () -> quorumLog(quorumId).subSequence(start, end));
  }

  @Override
  public ListenableFuture<Boolean> truncateLog(long seqNum, String quorumId) {
    getQuorumStructure(quorumId).setExpectedNextSequenceNumber(seqNum);
    oLogEntryOracle(quorumId).notifyTruncation(seqNum);
    return taskExecutor.submit(quorumId, () -> {
      quorumLog(quorumId).truncate(seqNum);
      return true;
    });
  }

  @Override
  public long getNextSeqNum(String quorumId) {
    return getQuorumStructure(quorumId).expectedNextSequenceNumber;
  }

  @Override
  public long getLastTerm(String quorumId) {
    return oLogEntryOracle(quorumId).getLastTerm();
  }

  @Override
  public long getLogTerm(long seqNum, String quorumId) {
    return oLogEntryOracle(quorumId).getTermAtSeqNum(seqNum);
  }

  @Override
  public QuorumConfigurationWithSeqNum getLastQuorumConfig(String quorumId) {
    return oLogEntryOracle(quorumId).getLastQuorumConfig();
  }

  @Override
  public void roll() throws IOException, ExecutionException, InterruptedException {
    // TODO implement roll()
  }

  @Override
  public void close() throws IOException {
    try {
      taskExecutor.shutdownAndAwaitTermination(15, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }

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

  private SequentialLog<OLogEntry> quorumLog(String quorumId) {
    return getQuorumStructure(quorumId).quorumLog;
  }

  private OLogEntryOracle oLogEntryOracle(String quorumId) {
    return getQuorumStructure(quorumId).oLogEntryOracle;
  }

  private PerQuorum getQuorumStructure(String quorumId) {
    PerQuorum perQuorum = quorumMap.get(quorumId);
    if (perQuorum == null || !perQuorum.opened) {
      quorumNotOpen(quorumId);
    }
    return perQuorum;
  }

  private void createAndPrepareQuorumStructures(String quorumId) {
    quorumMap.computeIfAbsent(quorumId, q -> {
      try {
        return new PerQuorum(quorumId);
      } catch (IOException e) {
        // Translate exception because computeIfAbsent 's Function declares no checked exception
        throw new RuntimeException(e);
      }
    });
  }

  private void updateOracleWithNewEntries(List<OLogEntry> entries, String quorumId) {
    OLogEntryOracle oLogEntryOracle = oLogEntryOracle(quorumId);
    for (OLogEntry e : entries) {
      oLogEntryOracle.notifyLogging(e);
    }
  }

  private void quorumNotOpen(String quorumId) {
    throw new QuorumNotOpen("QuorumDelegatingLog#getQuorumStructure: quorum " + quorumId + " not open");
  }
}
