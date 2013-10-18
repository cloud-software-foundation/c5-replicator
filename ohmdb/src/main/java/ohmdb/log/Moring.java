/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.log;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import ohmdb.generated.Log;
import ohmdb.replication.Raft;
import ohmdb.replication.RaftLogAbstraction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Moring implements RaftLogAbstraction {
  private final OLog log;
  long nodeId;
  HashMap<Long, Long> latestTombstones = new HashMap<>();
  long currentTerm = 0;
  long lastIndex = 0;

  Moring(OLog log, long nodeId) {
    this.nodeId = nodeId;
    this.log = log;
  }

  @Override
  public ListenableFuture<Boolean> logEntries(List<Raft.LogEntry> entries) {
    List<Log.OLogEntry> oLogEntries = new ArrayList<>();
    for (Raft.LogEntry entry : entries) {
      long idx = getNextIdxGreaterThan(0);
      oLogEntries.add(Log.OLogEntry
          .newBuilder()
          .setTombStone(false)
          .setTerm(entry.getTerm())
          .setIndex(idx)
          .setNodeId(nodeId)
          .setValue(entry.getData()).build());
    }
    return this.log.logEntry(oLogEntries, nodeId);
  }

  @Override
  public Raft.LogEntry getLogEntry(long index) {
    return this.log.getLogEntry(index, nodeId);
  }

  @Override
  public long getLogTerm(long index) {
    return this.log.getLogTerm(index, nodeId);
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
    updateInMemoryTombstone(nodeId, entryIndex);
    return this.log.truncateLog(entryIndex, nodeId);
  }

  private void updateInMemoryTombstone(long nodeId, long entryIndex) {
    if (!this.latestTombstones.containsKey(nodeId)) {
      this.latestTombstones.put(nodeId, entryIndex);
    } else {
      long latestIndex = this.latestTombstones.get(nodeId);
      if (latestIndex < entryIndex) {
        this.latestTombstones.put(nodeId, entryIndex);
      }
    }
  }

  public long getNextIdxGreaterThan( long min) {
    if (lastIndex < min) {
      lastIndex = min;
    } else {
      lastIndex++;
    }
    return lastIndex;
  }
}
