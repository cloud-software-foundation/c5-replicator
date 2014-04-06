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

import c5db.replication.generated.LogEntry;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * ReplicatorLog hosted in memory, e.g. for unit testing ReplicatorInstance in-memory. This
 * implementation just provides the basics needed to make the consensus algorithm work.
 */
public class InRamLog implements ReplicatorLog {

  private final List<LogEntry> log = new ArrayList<>();

  public InRamLog() {
  }

  @Override
  public ListenableFuture<Boolean> logEntries(List<LogEntry> entries) {
    // add them, for great justice.

    assert (entries.isEmpty()) || ((log.size() + 1) == entries.get(0).getIndex());
    // TODO more assertions

    log.addAll(entries);


    SettableFuture<Boolean> r = SettableFuture.create();
    r.set(true);
    return r;
  }

  @Override
  public ListenableFuture<LogEntry> getLogEntry(long index) {
    assert index > 0;

    SettableFuture<LogEntry> future = SettableFuture.create();
    if (index - 1 >= log.size()) {
      future.set(null);
    } else {
      future.set(log.get((int) index - 1));
    }
    return future;
  }

  @Override
  public ListenableFuture<List<LogEntry>> getLogEntries(long start, long end) {
    assert start > 0;
    assert end >= start;

    SettableFuture<List<LogEntry>> future = SettableFuture.create();
    future.set(log.subList((int) start - 1, (int) end - 1));
    return future;
  }

  @Override
  public synchronized long getLogTerm(long index) {
    assert index > 0;

    if (index - 1 >= log.size()) {
      return 0;
    }
    return log.get((int) index - 1).getTerm();
  }

  @Override
  public synchronized long getLastTerm() {
    if (log.isEmpty()) {
      return 0;
    }
    return log.get(log.size() - 1).getTerm();
  }

  @Override
  public synchronized long getLastIndex() {
    return log.size();
  }

  @Override
  public synchronized ListenableFuture<Boolean> truncateLog(long entryIndex) {
    log.subList((int) entryIndex - 1, log.size()).clear();
    SettableFuture<Boolean> r = SettableFuture.create();
    r.set(true);
    return r;
  }
}
