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

/**
 * Keeps track of, and provides answers about, election terms.
 */
public interface TermOracle {
  /**
   * Accept an entry's (seqNum, term) and possibly incorporate this information in the map. Sequence number and
   * term refer to the concepts used by the consensus algorithm. The consensus/election algorithm guarantees
   * that term is nondecreasing.
   *
   * @param seqNum Log seqNum
   * @param term   Log term
   */
  void notifyLogging(long seqNum, long term);

  /**
   * This method removes information from the map. It should be used when the log has truncated some entries.
   *
   * @param seqNum Index to truncate back to, inclusive.
   */
  void notifyTruncation(long seqNum);

  /**
   * Get the log term for a specified index, or zero if there is no preceding index such that the term can be
   * inferred.
   *
   * @param seqNum Log sequence number
   * @return The log term at this sequence number
   */
  long getTermAtSeqNum(long seqNum);
}

