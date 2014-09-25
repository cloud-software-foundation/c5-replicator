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

import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryIterable;

import java.io.IOException;
import java.util.List;

/**
 * Abstraction representing a sequence of log entries, persisted to some medium. Entries can
 * only be added to the sequence by appending, and entries can only be removed from the sequence
 * by truncating from the end. Entries cannot be changed in place (except by truncating and then
 * appending).
 * <p>
 * This structure does not have the notion of quorums; SequentialLog only contains one ascending
 * sequence of log entries.
 *
 * @param <E> Type of entry the log contains.
 */
public interface SequentialLog<E extends SequentialEntry> extends AutoCloseable, SequentialEntryIterable<E> {
  /**
   * Add entries to the log.
   *
   * @param entry Log entry to add.
   * @throws IOException
   */
  void append(List<E> entry) throws IOException;

  /**
   * Retrieve entries from the log. This method guarantees to return exactly (end - start) entries.
   * The entries returned are guaranteed to have ascending, consecutive sequence numbers.
   *
   * @param start The sequence number of the first entry to retrieve.
   * @param end   One beyond the sequence number of the last entry to retrieve. End must be greater
   *              than or equal to start.
   * @return A list of the requested entries.
   * @throws IOException, LogEntryNotFound, LogEntryNotInSequence
   */
  List<E> subSequence(long start, long end) throws IOException, LogEntryNotFound, LogEntryNotInSequence;

  /**
   * Return true if the log contains no entries.
   *
   * @return True if the log is empty, else false.
   * @throws IOException
   */
  boolean isEmpty() throws IOException;

  /**
   * Retrieve the last entry in the log, or null if the log is empty.
   *
   * @return The last entry.
   * @throws IOException
   */
  E getLastEntry() throws IOException;

  /**
   * Remove entries from the tail of the log.
   *
   * @param seqNum Remove every entry with sequence number greater than or equal to seqNum.
   * @throws IOException, LogEntryNotFound
   */
  void truncate(long seqNum) throws IOException, LogEntryNotFound;

  /**
   * Synchronously persist all previously written changes to the underlying medium.
   *
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Release any held resources. After calling close, any other operation will throw an exception.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Exception indicating a requested log entry was not found
   */
  class LogEntryNotFound extends Exception {
    public LogEntryNotFound(Throwable cause) {
      super(cause);
    }

    public LogEntryNotFound(String s) {
      super(s);
    }
  }

  /**
   * Exception indicating a log entry has been read with an incorrect sequence number.
   */
  class LogEntryNotInSequence extends Exception {
    public LogEntryNotInSequence() {
      super();
    }

    public LogEntryNotInSequence(String s) {
      super(s);
    }
  }
}