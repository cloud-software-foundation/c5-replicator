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

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;

import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;

/**
 * A write-ahead log for several quorums.
 */
public interface OLog extends AutoCloseable {

  /**
   * Prepare a new quorum for logging, and return notification when it is ready. If existing
   * information is found for this quorum, this method will find it and set up any internal
   * structures and/or caches necessary for the quorum.
   *
   * @param quorumId Quorum id
   * @return Future which will return when opening is complete. Any other I/O method call for
   * this quorum, prior to completion of the future, will throw an exception.
   */
  ListenableFuture<Void> openAsync(String quorumId);

  /**
   * Append the passed entries to the log. All calls to this method for a given quorum must be
   * serializable: in other words, if this method is called twice with the same quorumId, then
   * the caller must know that one method call "happens before" the other. Also, the passed
   * list must be non-null and non-empty.
   *
   * @param entries  Non-null list of zero or more entries.
   * @param quorumId Quorum id these entries should be logged under
   * @return Future indicating completion. Failure will be indicated by exception.
   * @throws java.lang.IllegalArgumentException when attempting to log an entry not in
   *                                            the correct sequence (for any given
   *                                            quorum, the sequence numbers must be
   *                                            strictly ascending with no gaps).
   */
  ListenableFuture<Boolean> logEntries(List<OLogEntry> entries, String quorumId);

  /**
   * Asynchronously retrieve a range of entries from sequence number 'start', inclusive, to
   * sequence number 'end', exclusive. Returns every entry in the specified range. Any
   * entries retrieved are guaranteed to have consecutive indices.
   *
   * @param start    First seqNum in range
   * @param end      One beyond the last seqNum in the desired range; must be greater than or
   *                 equal to start. If this equals start, a list of length zero will be
   *                 retrieved.
   * @param quorumId Quorum id of entries to retrieve
   * @return Future containing a list of log entries upon completion.
   */
  ListenableFuture<List<OLogEntry>> getLogEntries(long start, long end, String quorumId);

  /**
   * Logically delete entries from the tail of the log.
   *
   * @param entrySeqNum Delete entries back to, and including, the entry with this seqNum,
   * @param quorumId    Quorum id within which to delete entries
   * @return Future indicating completion.
   */
  ListenableFuture<Boolean> truncateLog(long entrySeqNum, String quorumId);

  /**
   * Gets the sequence number the log expects to receive next for the given quorum. After
   * logging an entry, the next sequence number will be the entry's sequence number plus one.
   *
   * @param quorumId Quorum id
   * @return The next sequence number for the given quorum.
   */
  long getNextSeqNum(String quorumId);

  /**
   * Gets the latest election term value in the log for the given quorum.
   *
   * @param quorumId Quorum id
   * @return the last term or 0 if no term has been established.
   */
  long getLastTerm(String quorumId);

  /**
   * Retrieve the "term" (i.e., leader or election term) corresponding to the given pair
   * (seqNum, quorum)
   *
   * @param seqNum   Log entry seqNum
   * @param quorumId Log entry quorum
   * @return The term for this entry, or zero if not found.
   */
  long getLogTerm(long seqNum, String quorumId);

  /**
   * Retrieve the latest quorum configuration and the sequence number on which it was
   * established.
   *
   * @param quorumId Log entry quorum
   * @return The quorum configuration and the sequence number on which it was created; or the
   * empty configuration and zero, respectively, if there is none.
   */
  QuorumConfigurationWithSeqNum getLastQuorumConfig(String quorumId);

  /**
   * Save off and close a quorum's log, and begin a new one. Any entries logged prior to
   * calling this method will end up in the old log, and any logged afterwards will end up
   * in the new log.
   * <p>
   * It is possible that a later truncation operation which truncates to a sequence number
   * present in the old log will undo this operation.
   *
   * @param quorumId Quorum id
   * @return A future which will return when the operation is complete, or else yield an
   * exception.
   * @throws IOException
   */
  ListenableFuture<Void> roll(String quorumId) throws IOException;

  /**
   * Dispose of held resources after completing any pending operations.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Exception indicating an operation was attempted on a log that was not open (for the
   * requested quorum).
   */
  class QuorumNotOpen extends RuntimeException {
    public QuorumNotOpen(String s) {
      super(s);
    }
  }
}
