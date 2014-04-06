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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Implementation of ReplicatorLog which delegates to an OLog. Each replicator instance has
 * a ReplicatorLog associated with it. The Mooring implementation allows each of these to
 * communicate to the same OLog behind the scenes. For instance, since the OLog API requires specifying
 * quorumId for most operations, whereas the ReplicatorLog does not "know about" quorumId,
 * Mooring bridges the gap by explicitly tracking quorumId and providing it on delegated calls.
 */
public class Mooring implements ReplicatorLog {
  final OLog log;
  final String quorumId;
  HashMap<String, Long> latestTombstones = new HashMap<>();
  long currentTerm = 0;
  long lastIndex = 0;

  Mooring(OLog log, String quorumId) {
    this.quorumId = quorumId;
    this.log = log;
  }

  @Override
  public ListenableFuture<Boolean> logEntries(List<LogEntry> entries) {
    List<OLogEntry> logEntries = new ArrayList<>();
    for (LogEntry entry : entries) {
      long idx = getNextIdxGreaterThan(0);
      logEntries.add(new OLogEntry(
          idx,
          entry.getTerm(),
          entry.getDataList()));
    }
    if (logEntries.size() > 0) {
      currentTerm = logEntries.get(logEntries.size() - 1).getElectionTerm();
    }
    return log.logEntry(logEntries, quorumId);
  }

  @Override
  public ListenableFuture<LogEntry> getLogEntry(long index) {
    return Futures.transform(log.getLogEntry(index, quorumId), OLogEntry::toProtostuffMessage);
  }

  @Override
  public ListenableFuture<List<LogEntry>> getLogEntries(long start, long end) {
    return Futures.transform(log.getLogEntries(start, end, quorumId), Mooring::toWireMessages);
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
    updateInMemoryTombstone(quorumId, entryIndex);
    return log.truncateLog(entryIndex, quorumId);
  }

  private void updateInMemoryTombstone(String quorumId, long entryIndex) {
    if (!this.latestTombstones.containsKey(quorumId)) {
      this.latestTombstones.put(quorumId, entryIndex);
    } else {
      long latestIndex = this.latestTombstones.get(quorumId);
      if (latestIndex < entryIndex) {
        this.latestTombstones.put(quorumId, entryIndex);
      }
    }
  }

  public long getNextIdxGreaterThan(long min) {
    if (lastIndex < min) {
      lastIndex = min;
    } else {
      lastIndex++;
    }
    return lastIndex;
  }

  private static List<LogEntry> toWireMessages(List<OLogEntry> entries) {
    return Lists.transform(entries, OLogEntry::toProtostuffMessage);
  }
}
