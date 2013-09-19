/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.replication;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * A log abstraction for RAFT.
 */
public interface RaftLogAbstraction {

    /**
     * Log an entry to the log.
     *
     * This might take a few so let's use a future.
     *
     * @param logData the raw data
     * @param term the term of when the entry was received by the leader
     * @param completionNotification the notification of logging success, or a Throwable if failed.
     * @return the log index of this entry.
     */
    public long logEntry(byte[] logData, long term, SettableFuture<Object> completionNotification);


    // These get info about the log.
    public byte[] getLogData(long index);

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
