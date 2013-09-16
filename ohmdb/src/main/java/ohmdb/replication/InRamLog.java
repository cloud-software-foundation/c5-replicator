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

import java.util.ArrayList;

/**

 */
public class InRamLog implements RaftLogAbstraction {

    private static class Entry {
        public final long index;
        public final long term;
        public final byte[] data;

        private Entry(long index, long term, byte[] data) {
            this.index = index;
            this.term = term;
            this.data = data;
        }
    }

    private final ArrayList<Entry> log = new ArrayList<>();

    public InRamLog() {
    }

    @Override
    public synchronized long logEntry(byte[] logData, long term) {
        int nextIdx = log.size();
        Entry e = new Entry(nextIdx, term, logData);
        log.add(e);
        return nextIdx;
    }

    @Override
    public synchronized byte[] getLogData(long index) {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public synchronized long getLogTerm(long index) {
        assert index < log.size();
        return log.get((int) index).term;
    }

    @Override
    public synchronized long getLastTerm() {
        return log.get(log.size()-1).term;
    }

    @Override
    public synchronized long getLastIndex() {
        return log.size();
    }

    @Override
    public synchronized void truncateLog(long entryIndex) {
        log.subList((int) entryIndex, log.size()).clear();
    }
}
