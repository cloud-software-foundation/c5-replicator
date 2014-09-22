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

/**
 * Keeps track of, and provides answers about, logged OLogEntries.
 */
public interface OLogEntryOracle {
  /**
   * Accept an OLogEntry and possibly incorporate it into the information tracked.
   * This method must be called for every OLogEntry logged, or else the OLogEntryOracle
   * will be out of sync with the log.
   *
   * @param entry Entry being logged.
   */
  void notifyLogging(OLogEntry entry);

  /**
   * This method removes information from the map. It must be called when the log
   * has truncated some entries.
   *
   * @param seqNum Log sequence number to truncate back to, inclusive.
   */
  void notifyTruncation(long seqNum);

  /**
   * Return the greatest sequence number of entries logged. If no entries have
   * been logged, return zero. Immediately after notifyTruncation is called, the
   * greatest sequence number will be one less than the sequence number passed to
   * notifyTruncation.
   *
   * @return The greatest sequence known to the oracle, or zero if there is none.
   */
  long getGreatestSeqNum();

  /**
   * Get the last term logged, or zero if there is none.
   *
   * @return The latest election term.
   */
  long getLastTerm();

  /**
   * Get the log term for a specified sequence number, or zero if the sequence number
   * is less than that of every entry logged.
   *
   * @param seqNum Log sequence number
   * @return The log term at this sequence number
   */
  long getTermAtSeqNum(long seqNum);

  /**
   * Get the last quorum configuration, together with the sequence number at which it was
   * established. If there is none, return the empty quorum configuration and a seqNum
   * of zero.
   *
   * @return The quorum configuration active at this sequence number, and the sequence
   * number at which it became active.
   */
  QuorumConfigurationWithSeqNum getLastQuorumConfig();


  interface OLogEntryOracleFactory {
    OLogEntryOracle create();
  }

  class QuorumConfigurationWithSeqNum {
    public final QuorumConfiguration quorumConfiguration;
    public final long seqNum;

    public QuorumConfigurationWithSeqNum(QuorumConfiguration quorumConfiguration, long seqNum) {
      this.quorumConfiguration = quorumConfiguration;
      this.seqNum = seqNum;
    }

    @Override
    public String toString() {
      return "QuorumConfigurationWithSeqNum{" +
          "quorumConfiguration=" + quorumConfiguration +
          ", seqNum=" + seqNum +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      QuorumConfigurationWithSeqNum that = (QuorumConfigurationWithSeqNum) o;

      return seqNum == that.seqNum
          && quorumConfiguration.equals(that.quorumConfiguration);
    }

    @Override
    public int hashCode() {
      int result = quorumConfiguration != null ? quorumConfiguration.hashCode() : 0;
      result = 31 * result + (int) (seqNum ^ (seqNum >>> 32));
      return result;
    }
  }
}

