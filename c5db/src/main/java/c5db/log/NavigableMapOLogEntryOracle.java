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

import c5db.generated.OLogContentType;
import c5db.replication.QuorumConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * OLogEntryOracle using a NavigableMap, and not persisting any of its data.
 */
public class NavigableMapOLogEntryOracle implements OLogEntryOracle {
  private static final Logger LOG = LoggerFactory.getLogger(NavigableMapOLogEntryOracle.class);

  private final NavigableMap<Long, Long> termMap = new TreeMap<>();
  private final NavigableMap<Long, QuorumConfiguration> configMap = new TreeMap<>();

  private long greatestSeqNum = 0;

  @Override
  public void notifyLogging(OLogEntry entry) {
    final long lastTerm = getLastTerm();
    final long entryTerm = entry.getElectionTerm();
    final long entrySeqNum = entry.getSeqNum();

    ensureNondecreasingTerm(entryTerm, lastTerm);

    greatestSeqNum = Math.max(greatestSeqNum, entrySeqNum);

    if (entryTerm > lastTerm) {
      termMap.put(entrySeqNum, entryTerm);
    }

    if (entry.getContent().getType() == OLogContentType.QUORUM_CONFIGURATION) {
      configMap.put(entrySeqNum,
          QuorumConfiguration.fromProtostuff(entry.toProtostuff().getQuorumConfiguration()));
    }
  }

  @Override
  public void notifyTruncation(long seqNum) {
    termMap.tailMap(seqNum, true).clear();
    configMap.tailMap(seqNum, true).clear();
    greatestSeqNum = seqNum - 1;
  }

  @Override
  public long getGreatestSeqNum() {
    return greatestSeqNum;
  }

  @Override
  public long getLastTerm() {
    if (termMap.isEmpty()) {
      return 0;
    } else {
      return termMap.lastEntry().getValue();
    }
  }

  @Override
  public long getTermAtSeqNum(long seqNum) {
    final Map.Entry<Long, Long> entry = termMap.floorEntry(seqNum);
    return entry == null ? 0 : entry.getValue();
  }

  @Override
  public QuorumConfigurationWithSeqNum getLastQuorumConfig() {
    if (configMap.isEmpty()) {
      return new QuorumConfigurationWithSeqNum(QuorumConfiguration.EMPTY, 0);
    } else {
      final Map.Entry<Long, QuorumConfiguration> lastEntry = configMap.lastEntry();
      return new QuorumConfigurationWithSeqNum(lastEntry.getValue(), lastEntry.getKey());
    }
  }

  private void ensureNondecreasingTerm(long entryTerm, long lastTerm) {
    if (entryTerm < lastTerm) {
      LOG.error("Encountered a decreasing term, {}, where the last known term was {}", entryTerm, lastTerm);
      throw new IllegalArgumentException("Decreasing term number");
    }
  }
}
