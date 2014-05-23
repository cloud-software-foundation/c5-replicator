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

import c5db.replication.QuorumConfiguration;
import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static c5db.log.SequentialLog.LogEntryNotFound;
import static c5db.log.SequentialLog.LogEntryNotInSequence;

/**
 * ReplicatorLog hosted in memory, e.g. for unit testing ReplicatorInstance in-memory. This
 * implementation just provides the basics needed to make the consensus algorithm work.
 */
public class InRamLog implements ReplicatorLog {

  private final List<LogEntry> log = new ArrayList<>();

  public InRamLog() {
  }

  @Override
  public synchronized ListenableFuture<Boolean> logEntries(List<LogEntry> entries) {
    validateEntries(entries);
    log.addAll(entries);

    return blockingFuture(true);
  }

  @Override
  public synchronized ListenableFuture<LogEntry> getLogEntry(long index) {
    assert index > 0;

    return blockingFuture(getEntryInternal(index));
  }

  @Override
  public synchronized ListenableFuture<List<LogEntry>> getLogEntries(long start, long end) {
    assert start > 0;
    assert end >= start;

    List<LogEntry> foundEntries = log.stream()
        .filter((entry) -> start <= entry.getIndex() && entry.getIndex() < end)
        .collect(Collectors.toList());

    if (foundEntries.size() != (end - start)) {
      throw new LogEntryNotFound("requested [" + start + ", " + end + "); received" + foundEntries.toString());
    }

    return blockingFuture(foundEntries);
  }

  @Override
  public synchronized long getLogTerm(long index) {
    assert index > 0;

    Optional<LogEntry> requestedEntry = optionallyGetEntryInternal(index);

    if (requestedEntry.isPresent()) {
      return requestedEntry.get().getTerm();
    } else {
      return 0;
    }
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
    if (log.isEmpty()) {
      return 0;
    }
    return log.get(log.size() - 1).getIndex();
  }

  @Override
  public synchronized ListenableFuture<Boolean> truncateLog(long entryIndex) {
    LogEntry firstRemovedEntry = getEntryInternal(entryIndex);
    int listIndex = log.lastIndexOf(firstRemovedEntry);
    log.subList(listIndex, log.size()).clear();

    return blockingFuture(true);
  }

  @Override
  public synchronized QuorumConfiguration getLastConfiguration() {
    for (LogEntry entry : Lists.reverse(log)) {
      if (entry.getQuorumConfiguration() != null) {
        return QuorumConfiguration.fromProtostuff(entry.getQuorumConfiguration());
      }
    }

    return QuorumConfiguration.EMPTY;
  }

  @Override
  public synchronized long getLastConfigurationIndex() {
    for (LogEntry entry : Lists.reverse(log)) {
      if (entry.getQuorumConfiguration() != null) {
        return entry.getIndex();
      }
    }

    return 0;
  }

  private void validateEntries(List<LogEntry> entries) {
    // Ensure ascending with no gaps
    long lastIndex = getLastIndex();
    for (LogEntry e : entries) {
      if (lastIndex == 0 || e.getIndex() == lastIndex + 1) {
        lastIndex = e.getIndex();
      } else {
        throw new LogEntryNotInSequence("entries not in sequence: " + entries.toString());
      }
    }
  }

  private Optional<LogEntry> optionallyGetEntryInternal(long index) {
    return log.stream()
        .filter((entry) -> entry.getIndex() == index)
        .findFirst();
  }

  private LogEntry getEntryInternal(long index) {
    Optional<LogEntry> requestedEntry = optionallyGetEntryInternal(index);

    if (!requestedEntry.isPresent()) {
      throw new LogEntryNotFound("entry index " + index + " not found");
    }

    return requestedEntry.get();
  }

  private static <V> ListenableFuture<V> blockingFuture(V result) {
    SettableFuture<V> future = SettableFuture.create();
    new Thread(() -> future.set(result))
        .start();
    return future;
  }
}
