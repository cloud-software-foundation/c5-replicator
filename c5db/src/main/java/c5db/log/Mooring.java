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
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * Implementation of ReplicatorLog which delegates to an OLog. Each replicator instance has
 * a ReplicatorLog associated with it. The Mooring implementation allows each of these to
 * communicate to the same OLog behind the scenes. For instance, since the OLog API requires specifying
 * quorumId for most operations, whereas the ReplicatorLog does not "know about" quorumId,
 * Mooring bridges the gap by explicitly tracking quorumId and providing it on delegated calls.
 * <p>
 * Mooring also caches the current term and the last index (log sequence number) so that in
 * most cases these never need to access OLog.
 */
public class Mooring implements ReplicatorLog {
  private static final int LOG_TIMEOUT = 10; // seconds
  private final OLog log;
  private final String quorumId;

  private long currentTerm;
  private long lastIndex;

  private final NavigableMap<Long, QuorumConfiguration> configurations = new TreeMap<>();

  Mooring(OLog log, String quorumId) throws IOException {
    this.quorumId = quorumId;
    this.log = log;
    configurations.put(0L, QuorumConfiguration.EMPTY);

    try {
      this.currentTerm = log.getLastTerm(quorumId).get(LOG_TIMEOUT, TimeUnit.SECONDS);
      this.lastIndex = log.getLastSeqNum(quorumId).get(LOG_TIMEOUT, TimeUnit.SECONDS);
      // TODO get last configuration as well.
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public ListenableFuture<Boolean> logEntries(List<LogEntry> entries) {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("Mooring#logEntries: empty entry list");
    }

    List<OLogEntry> oLogEntries = new ArrayList<>();
    for (LogEntry entry : entries) {
      if (isAConfigurationEntry(entry)) {
        configurations.put(entry.getIndex(), QuorumConfiguration.fromProtostuff(entry.getQuorumConfiguration()));
      }
      oLogEntries.add(OLogEntry.fromProtostuff(entry));
    }

    updateCachedTermAndIndex(oLogEntries);

    return log.logEntry(oLogEntries, quorumId);
  }

  @Override
  public ListenableFuture<LogEntry> getLogEntry(long index) {
    return Futures.transform(log.getLogEntry(index, quorumId), OLogEntry::toProtostuff);
  }

  @Override
  public ListenableFuture<List<LogEntry>> getLogEntries(long start, long end) {
    return Futures.transform(log.getLogEntries(start, end, quorumId), Mooring::toProtostuffMessages);
  }

  @Override
  public long getLogTerm(long index) {
    return log.getLogTerm(index, quorumId);
  }

  @Override
  public long getLastTerm() {
    return currentTerm;
  }

  @Override
  public long getLastIndex() {
    return lastIndex;
  }

  @Override
  public ListenableFuture<Boolean> truncateLog(long entryIndex) {
    if (entryIndex <= 0) {
      throw new IllegalArgumentException("Mooring#truncateLog");
    }

    lastIndex = max(entryIndex - 1, 0);
    currentTerm = log.getLogTerm(lastIndex, quorumId);
    configurations.tailMap(entryIndex, true).clear();
    return log.truncateLog(entryIndex, quorumId);
  }

  @Override
  public QuorumConfiguration getLastConfiguration() {
    return configurations.lastEntry().getValue();
  }

  @Override
  public long getLastConfigurationIndex() {
    return configurations.lastKey();
  }


  private static List<LogEntry> toProtostuffMessages(List<OLogEntry> entries) {
    return Lists.transform(entries, OLogEntry::toProtostuff);
  }

  private void updateCachedTermAndIndex(List<OLogEntry> entriesToLog) {
    long size = entriesToLog.size();
    if (size > 0) {
      final OLogEntry lastEntry = entriesToLog.get(entriesToLog.size() - 1);
      currentTerm = lastEntry.getElectionTerm();
      lastIndex = lastEntry.getSeqNum();
    }
  }

  private boolean isAConfigurationEntry(LogEntry entry) {
    return entry.getQuorumConfiguration() != null;
  }
}
