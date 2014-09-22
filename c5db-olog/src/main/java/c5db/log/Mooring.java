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

import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;
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

  private QuorumConfiguration lastQuorumConfig = QuorumConfiguration.EMPTY;
  private long lastQuorumConfigIndex = 0;

  Mooring(OLog log, String quorumId) throws IOException {
    this.quorumId = quorumId;
    this.log = log;

    try {
      // TODO maybe move this from the constructor to an 'open' method
      log.openAsync(quorumId)
          .get(LOG_TIMEOUT, TimeUnit.SECONDS);

      currentTerm = log.getLastTerm(quorumId);
      lastIndex = log.getNextSeqNum(quorumId) - 1;

      setQuorumConfigFromLog();

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
        lastQuorumConfig = QuorumConfiguration.fromProtostuff(entry.getQuorumConfiguration());
        lastQuorumConfigIndex = entry.getIndex();
      }
      oLogEntries.add(OLogEntry.fromProtostuff(entry));
    }

    updateCachedTermAndIndex(oLogEntries);

    return log.logEntries(oLogEntries, quorumId);
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
    ListenableFuture<Boolean> truncateFuture = log.truncateLog(entryIndex, quorumId);
    setQuorumConfigFromLog();
    return truncateFuture;
  }

  @Override
  public QuorumConfiguration getLastConfiguration() {
    return lastQuorumConfig;
  }

  @Override
  public long getLastConfigurationIndex() {
    return lastQuorumConfigIndex;
  }

  private void setQuorumConfigFromLog() {
    final QuorumConfigurationWithSeqNum configFromLog = log.getLastQuorumConfig(quorumId);
    lastQuorumConfig = configFromLog.quorumConfiguration;
    lastQuorumConfigIndex = configFromLog.seqNum;
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
