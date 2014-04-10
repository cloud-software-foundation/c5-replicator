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

import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * Implementation of ReplicatorLog which delegates to an OLog. Each replicator instance has
 * a ReplicatorLog associated with it. The Mooring implementation allows each of these to
 * communicate to the same OLog behind the scenes. For instance, since the OLog API requires specifying
 * quorumId for most operations, whereas the ReplicatorLog does not "know about" quorumId,
 * Mooring bridges the gap by explicitly tracking quorumId and providing it on delegated calls.
 *
 * Mooring also caches the current term and the last index (log sequence number) so that in
 * most cases these never need to access OLog.
 */
public class Mooring implements ReplicatorLog {
  private static final int LOG_TIMEOUT = 10; // seconds
  final OLog log;
  final String quorumId;
  long currentTerm;
  long lastIndex;

  Mooring(OLog log, String quorumId) throws IOException {
    this.quorumId = quorumId;
    this.log = log;
    try {
      this.currentTerm = log.getLastTerm(quorumId).get(LOG_TIMEOUT, TimeUnit.SECONDS);
      this.lastIndex = log.getLastSeqNum(quorumId).get(LOG_TIMEOUT, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public ListenableFuture<Boolean> logEntries(List<LogEntry> entries) {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("Mooring#logEntries: empty entry list");
    }

    List<OLogEntry> logEntries = Lists.transform(entries, OLogEntry::fromProtostuffMessage);

    updateCachedTermAndIndex(logEntries);

    return log.logEntry(logEntries, quorumId);
  }

  @Override
  public ListenableFuture<LogEntry> getLogEntry(long index) {
    return Futures.transform(log.getLogEntry(index, quorumId), OLogEntry::toProtostuffMessage);
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
    lastIndex = max(entryIndex - 1, 0);
    currentTerm = log.getLogTerm(lastIndex, quorumId);
    return log.truncateLog(entryIndex, quorumId);
  }

  private static List<LogEntry> toProtostuffMessages(List<OLogEntry> entries) {
    return Lists.transform(entries, OLogEntry::toProtostuffMessage);
  }

  private void updateCachedTermAndIndex(List<OLogEntry> entriesToLog) {
    long size = entriesToLog.size();
    if (size > 0) {
      final OLogEntry lastEntry = entriesToLog.get(entriesToLog.size() - 1);
      currentTerm = lastEntry.getElectionTerm();
      lastIndex = lastEntry.getSeqNum();
    }
  }
}
