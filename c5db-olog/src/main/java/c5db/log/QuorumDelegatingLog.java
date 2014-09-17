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

import c5db.LogConstants;
import c5db.generated.OLogHeader;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.util.C5Iterators;
import c5db.util.CheckedSupplier;
import c5db.util.KeySerializingExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigatorFactory;
import static c5db.log.OLogEntryOracle.OLogEntryOracleFactory;
import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;
import static c5db.log.SequentialLog.LogEntryNotFound;
import static c5db.log.SequentialLog.LogEntryNotInSequence;

/**
 * OLog that delegates each quorum's logging tasks to a separate log record for that quorum, executing
 * any blocking tasks on a KeySerializingExecutor, with quorumId as the key. It is safe for use
 * by multiple threads, but each quorum's sequence numbers must be ascending with no gaps within
 * that quorum; so having multiple unsynchronized threads writing for the same quorum is unlikely
 * to work.
 * <p>
 * Each quorum's log record is a sequence of SequentialLogs, each based on its own persistence (e.g.,
 * a file) served from the LogPersistenceService injected on creation.
 */
public class QuorumDelegatingLog implements OLog, AutoCloseable {
  private final LogPersistenceService<?> persistenceService;
  private final KeySerializingExecutor taskExecutor;
  private final Map<String, PerQuorum> quorumMap = new ConcurrentHashMap<>();

  private final OLogEntryOracleFactory OLogEntryOracleFactory;
  private final PersistenceNavigatorFactory persistenceNavigatorFactory;

  public QuorumDelegatingLog(LogPersistenceService<?> persistenceService,
                             KeySerializingExecutor taskExecutor,
                             OLogEntryOracleFactory OLogEntryOracleFactory,
                             PersistenceNavigatorFactory persistenceNavigatorFactory
  ) {
    this.persistenceService = persistenceService;
    this.taskExecutor = taskExecutor;
    this.OLogEntryOracleFactory = OLogEntryOracleFactory;
    this.persistenceNavigatorFactory = persistenceNavigatorFactory;
  }

  @Override
  public ListenableFuture<Void> openAsync(String quorumId) {
    quorumMap.computeIfAbsent(quorumId, q -> new PerQuorum(quorumId));
    return submitQuorumTask(quorumId, () -> {
      getQuorumStructure(quorumId).open();
      return null;
    });
  }

  @Override
  public ListenableFuture<Boolean> logEntries(List<OLogEntry> passedInEntries, String quorumId) {
    List<OLogEntry> entries = validateAndMakeDefensiveCopy(passedInEntries);

    getQuorumStructure(quorumId).ensureEntriesAreConsecutive(entries);
    updateOracleWithNewEntries(entries, quorumId);

    return submitQuorumTask(quorumId, () -> {
      currentLog(quorumId).append(entries);
      maybeSyncLogForQuorum(quorumId);
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

    return submitQuorumTask(quorumId, () -> {
      if (!seqNumPrecedesLog(start, getQuorumStructure(quorumId).currentLogWithHeader())) {
        return currentLog(quorumId).subSequence(start, end);
      } else {
        return multiLogGet(start, end, quorumId);
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> truncateLog(long seqNum, String quorumId) {
    getQuorumStructure(quorumId).setExpectedNextSequenceNumber(seqNum);
    oLogEntryOracle(quorumId).notifyTruncation(seqNum);

    return submitQuorumTask(quorumId, () -> {
      while (seqNumPrecedesLog(seqNum, getQuorumStructure(quorumId).currentLogWithHeader())) {
        getQuorumStructure(quorumId).deleteCurrentLog();
      }

      currentLog(quorumId).truncate(seqNum);
      maybeSyncLogForQuorum(quorumId);
      return true;
    });
  }

  @Override
  public long getNextSeqNum(String quorumId) {
    return getQuorumStructure(quorumId).getExpectedNextSequenceNumber();
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
  public ListenableFuture<Void> roll(String quorumId) throws IOException {
    final OLogHeader newLogHeader = buildRollHeader(quorumId);

    return submitQuorumTask(quorumId, () -> {
      getQuorumStructure(quorumId).roll(newLogHeader);
      return null;
    });
  }

  @Override
  public void close() throws IOException {
    try {
      taskExecutor.shutdownAndAwaitTermination(LogConstants.LOG_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    for (PerQuorum quorumStructure : quorumMap.values()) {
      quorumStructure.close();
    }
  }

  /**
   * A structure representing all information about the log record for a given quorum, and
   * methods to access its individual persistence objects (using SequentialLogWithHeader
   * instances). Each quorum has its own instance of PerQuorum. When the PerQuorum is
   * opened, it loads the current such object, if there is one, or else is creates a new
   * one. Additional objects are only loaded or created as necessary.
   * <p>
   * TODO This could use more clarity about its concurrency safety; perhaps methods which
   * TODO  must be called synchronously by the user of the QuorumDelegatingLog should be
   * TODO  broken out into their own class?
   */
  private class PerQuorum {
    private final String quorumId;
    private final Deque<SequentialLogWithHeader> logDeque = new LinkedList<>();

    /**
     * These fields may only be accessed synchronously with the caller of the QuorumDelegatingLog
     * public methods; in other words, they may not be accessed from an executing task.
     */
    private volatile long expectedNextSequenceNumber = 1;
    public final OLogEntryOracle oLogEntryOracle = OLogEntryOracleFactory.create();

    public PerQuorum(String quorumId) {
      this.quorumId = quorumId;
    }

    public void open() throws IOException {
      loadCurrentOrNewLog();
    }

    public void ensureEntriesAreConsecutive(List<OLogEntry> entries) {
      for (OLogEntry e : entries) {
        long entrySeqNum = e.getSeqNum();
        long expectedSeqNum = expectedNextSequenceNumber;
        if (entrySeqNum != expectedSeqNum) {
          throw new IllegalArgumentException("Unexpected sequence number in entries requested to be logged");
        }
        expectedNextSequenceNumber++;
      }
    }

    public void setExpectedNextSequenceNumber(long seqNum) {
      expectedNextSequenceNumber = seqNum;
    }

    public long getExpectedNextSequenceNumber() {
      return expectedNextSequenceNumber;
    }

    @NotNull
    public SequentialLogWithHeader currentLogWithHeader() throws IOException {
      if (logDeque.isEmpty()) {
        loadCurrentOrNewLog();
      }
      return logDeque.peek();
    }

    public void roll(OLogHeader newLogHeader) throws IOException {
      SequentialLogWithHeader newLog = SequentialLogWithHeader.writeNewLog(persistenceService,
          persistenceNavigatorFactory, newLogHeader, quorumId);
      logDeque.push(newLog);
    }

    public void deleteCurrentLog() throws IOException {
      persistenceService.truncate(quorumId);
      logDeque.pop();
    }

    public Iterator<SequentialLogWithHeader> getLogIterator() throws IOException {
      final Iterator<SequentialLogWithHeader> dequeIterator = logDeque.iterator();
      final int dequeSize = logDeque.size();

      // First return the log(s) already in memory, then read additional logs from the persistence.
      return Iterators.concat(
          dequeIterator,
          Iterators.transform(C5Iterators.advanced(persistenceService.getList(quorumId).iterator(), dequeSize),
              (persistenceSupplier) -> {
                try {
                  return SequentialLogWithHeader.readLogFromPersistence(persistenceSupplier.get(),
                      persistenceNavigatorFactory);
                } catch (IOException e) {
                  throw new IteratorIOException(e);
                }
              }));
    }

    public void close() throws IOException {
      // TODO if one log fails to close, it won't attempt to close any after that one.
      for (SequentialLogWithHeader logWithHeader : logDeque) {
        logWithHeader.log.close();
      }
    }

    private void loadCurrentOrNewLog() throws IOException {
      final BytePersistence persistence = persistenceService.getCurrent(quorumId);
      final SequentialLogWithHeader logWithHeader;

      if (persistence == null) {
        logWithHeader = SequentialLogWithHeader.writeNewLog(persistenceService, persistenceNavigatorFactory,
            newQuorumHeader(), quorumId);
      } else {
        logWithHeader = SequentialLogWithHeader.readLogFromPersistence(persistence, persistenceNavigatorFactory);
      }

      logDeque.push(logWithHeader);
      prepareLogOracle(logWithHeader);
      increaseExpectedNextSeqNumTo(oLogEntryOracle.getGreatestSeqNum() + 1);
    }

    private void prepareLogOracle(SequentialLogWithHeader logWithHeader) throws IOException {
      SequentialLog<OLogEntry> log = logWithHeader.log;
      final OLogHeader header = logWithHeader.header;

      oLogEntryOracle.notifyLogging(new OLogEntry(header.getBaseSeqNum(), header.getBaseTerm(),
          new OLogProtostuffContent<>(header.getBaseConfiguration())));
      // TODO it isn't necessary to read the content of every entry; only those which refer to configurations.
      // TODO Also should the navigator be updated on the last entry?
      log.forEach(oLogEntryOracle::notifyLogging);
    }

    private void increaseExpectedNextSeqNumTo(long seqNum) {
      if (seqNum > expectedNextSequenceNumber) {
        setExpectedNextSequenceNumber(seqNum);
      }
    }
  }

  /**
   * Exception thrown if an IOException occurs during
   */
  class IteratorIOException extends RuntimeException {
    public IteratorIOException(Throwable cause) {
      super(cause);
    }
  }

  private List<OLogEntry> validateAndMakeDefensiveCopy(List<OLogEntry> entries) {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("Attempting to log an empty entry list");
    }

    return ImmutableList.copyOf(entries);
  }

  private List<OLogEntry> multiLogGet(long start, long end, String quorumId)
      throws IOException, LogEntryNotFound, LogEntryNotInSequence {
    final Iterator<SequentialLogWithHeader> logIterator = getQuorumStructure(quorumId).getLogIterator();
    final Deque<List<OLogEntry>> entries = new LinkedList<>();

    long remainingEnd = end;

    while (remainingEnd > start) {
      if (!logIterator.hasNext()) {
        throw new LogEntryNotFound("Unable to locate a log containing the requested entries");
      }

      SequentialLogWithHeader logWithHeader = logIterator.next();
      long firstSeqNumInLog = logWithHeader.header.getBaseSeqNum() + 1;

      if (remainingEnd > firstSeqNumInLog) {
        entries.push(logWithHeader.log.subSequence(Math.max(firstSeqNumInLog, start), remainingEnd));
        remainingEnd = firstSeqNumInLog;
      }
    }

    return Lists.newArrayList(Iterables.concat(entries));
  }

  private void maybeSyncLogForQuorum(String quorumId) throws IOException {
    if (LogConstants.LOG_USE_FILE_CHANNEL_FORCE) {
      currentLog(quorumId).sync();
    }
  }

  private SequentialLog<OLogEntry> currentLog(String quorumId) throws IOException {
    return getQuorumStructure(quorumId).currentLogWithHeader().log;
  }

  private OLogEntryOracle oLogEntryOracle(String quorumId) {
    return getQuorumStructure(quorumId).oLogEntryOracle;
  }

  private PerQuorum getQuorumStructure(String quorumId) {
    PerQuorum perQuorum = quorumMap.get(quorumId);
    if (perQuorum == null) {
      quorumNotOpen(quorumId);
    }
    return perQuorum;
  }

  private void updateOracleWithNewEntries(List<OLogEntry> entries, String quorumId) {
    OLogEntryOracle oLogEntryOracle = oLogEntryOracle(quorumId);
    for (OLogEntry e : entries) {
      oLogEntryOracle.notifyLogging(e);
    }
  }

  private OLogHeader buildRollHeader(String quorumId) {
    final long baseTerm = getLastTerm(quorumId);
    final long baseSeqNum = getNextSeqNum(quorumId) - 1;
    final QuorumConfiguration baseConfiguration = getLastQuorumConfig(quorumId).quorumConfiguration;

    return new OLogHeader(baseTerm, baseSeqNum, baseConfiguration.toProtostuff());
  }

  private OLogHeader newQuorumHeader() {
    return new OLogHeader(0, 0, QuorumConfiguration.EMPTY.toProtostuff());
  }

  private boolean seqNumPrecedesLog(long seqNum, @NotNull SequentialLogWithHeader logWithHeader) {
    return seqNum <= logWithHeader.header.getBaseSeqNum();
  }

  private void quorumNotOpen(String quorumId) {
    throw new QuorumNotOpen("QuorumDelegatingLog#getQuorumStructure: quorum " + quorumId + " not open");
  }

  private <T> ListenableFuture<T> submitQuorumTask(String quorumId, CheckedSupplier<T, Exception> task) {
    return taskExecutor.submit(quorumId, task);
  }
}
