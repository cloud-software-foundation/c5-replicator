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