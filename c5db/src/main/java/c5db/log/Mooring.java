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

import c5db.generated.Log;
import c5db.replication.RaftLogAbstraction;
import c5db.replication.generated.Raft;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Mooring implements RaftLogAbstraction {
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
    public ListenableFuture<Boolean> logEntries(List<Raft.LogEntry> entries) {
        List<Log.OLogEntry> oLogEntries = new ArrayList<>();
        for (Raft.LogEntry entry : entries) {
            long idx = getNextIdxGreaterThan(0);
            oLogEntries.add(Log.OLogEntry
                    .newBuilder()
                    .setTombStone(false)
                    .setTerm(entry.getTerm())
                    .setIndex(idx)
                    .setQuorumId(quorumId)
                    .setValue(entry.getData()).build());
        }
        return this.log.logEntry(oLogEntries, quorumId);
    }

    @Override
    public Raft.LogEntry getLogEntry(long index) {
        return this.log.getLogEntry(index, quorumId);
    }

    @Override
    public long getLogTerm(long index) {
        return this.log.getLogTerm(index, quorumId);
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
        return this.log.truncateLog(entryIndex, quorumId);
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
}
