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
package c5db.replication;

import c5db.replication.generated.LogEntry;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * A log abstraction for the raft replicator.
 *
 * As a raft replicator I promise not to call from more than 1 thread.
 */
public interface RaftLogAbstraction {
    /**
     * Log entries to the log.  The entries are in order, and should start from getLastIndex() + 1.
     *
     * The implementation should feel free to verify this.
     *
     * After this call returns, the log implementation should mark these entries as "to be committed"
     * and calls to 'getLastIndex()' should return the last entry in "entries".  The implementation will
     * then return a future that will be set with either a 'new Object()' during success, or a Throwable
     * indicating the error after the log has been synced to disk.
     *
     * Note that over time, multiple calls to logEntries() may be issued before the prior call has signaled
     * full sync to the client.  This also implies that once this call returns, calls to the other methods
     * of this interface must now return data from these entries.  For example calling getLogTerm(long) should
     * return data from these entries even if they haven't been quite sync'ed to disk yet.
     *
     * @param entries new log entries
     * @return an future that indicates success.
     */
    public ListenableFuture<Boolean> logEntries(List<LogEntry> entries);


    public LogEntry getLogEntry(long index);

    /**
     * Get the term for a given log index.  This is expected to be fast, so its an
     * synchronous interface
     *
     * @param index
     * @return
     */
    public long getLogTerm(long index);

    // get the last term from the log

    /**
     * gets the term value for the last entry in the log. if the log is empty, then this will return
     * 0. A term value of 0 should never be valid.
     * @return the last term or 0 if no such entry
     */
    public long getLastTerm();

    /**
     * Gets the index of the most recent log entry.  An index is like a log sequence number, but there are
     * no holes.
     *
     * @return the index or 0 if the log is empty. This implies log entries start at 1.
     */
    public long getLastIndex();

    /**
     * Delete all log entries after and including the specified index.
     *
     * To persist the deletion, this might take a few so use a future.
     *
     * @param entryIndex the index entry to truncate log from.
     * @return a true or false depending if successful or not.
     */
    public ListenableFuture<Boolean> truncateLog(long entryIndex);
}
