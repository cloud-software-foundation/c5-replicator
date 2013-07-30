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

/**
 * A log abstraction for RAFT.
 */
public interface RaftLogAbstraction {

    /**
     * Log an entry to the log.
     *
     * @param logData the raw data
     * @param term the term of when the entry was recieved by the leader
     * @return the log-index entry of this log entry.
     */
    public long logEntry(byte[] logData, long term);


    // These get info about the log.
    public byte[] getLogData(long index);
    public long getLogTerm(long index);

    // get the last term from the log
    public long getLastTerm();
    public long getLastIndex();

}
