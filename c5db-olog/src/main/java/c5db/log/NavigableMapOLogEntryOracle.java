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
import c5db.log.generated.OLogContentType;
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
