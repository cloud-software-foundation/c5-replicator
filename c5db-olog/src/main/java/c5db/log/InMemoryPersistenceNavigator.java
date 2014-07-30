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

import c5db.LogConstants;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.NavigableMap;
import java.util.TreeMap;

import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceReader;
import static c5db.log.SequentialLog.LogEntryNotFound;

/**
 * PersistenceNavigator using only in-memory structures, not itself persisting any data it
 * has been issued by notifyLogging(). This class keeps an internal Navigable map from entry sequence
 * number to byte position. The strategy used is: when notifyLogging is called, if the entry
 * sequence number is at least k greater than the greatest entry sequence number already stored,
 * then store it. k is a configurable parameter, maxEntrySeek. Also, if requested to get the
 * address of a specific entry, and that address is not already stored, store it once it is
 * found.
 */
public class InMemoryPersistenceNavigator<E extends SequentialEntry> implements PersistenceNavigator {

  private final BytePersistence persistence;
  private final SequentialEntryCodec<E> codec;

  private final NavigableMap<Long, Long> index = new TreeMap<>();
  private final long fileOffset;
  private int maxEntrySeek = LogConstants.LOG_NAVIGATOR_DEFAULT_MAX_ENTRY_SEEK;

  public InMemoryPersistenceNavigator(BytePersistence persistence, SequentialEntryCodec<E> codec) {
    this(persistence, codec, 0);
  }

  public InMemoryPersistenceNavigator(BytePersistence persistence, SequentialEntryCodec<E> codec, long offset) {
    this.persistence = persistence;
    this.codec = codec;
    this.fileOffset = offset;

    // Logic is simplified if the index NavigableMap is guaranteed to have at least one entry.
    index.put(0L, 0L);
  }

  public void setMaxEntrySeek(int numberOfEntries) {
    if (numberOfEntries < 1) {
      throw new IllegalArgumentException("InMemoryPersistenceNavigator#setMaxEntrySeek");
    }
    maxEntrySeek = numberOfEntries;
  }

  @Override
  public void notifyLogging(long seqNum, long byteAddress) throws IOException {
    maybeAddToIndex(seqNum, byteAddress);
  }

  @Override
  public void addToIndex(long seqNum, long address) {
    index.put(seqNum, address);
  }

  @Override
  public void notifyTruncation(long seqNum) throws IOException {
    if (seqNum <= 0) {
      throw new IllegalArgumentException("InMemoryPersistenceNavigator#notifyTruncation");
    }
    truncateIndex(seqNum);
  }

  @Override
  public long getAddressOfEntry(long seqNum) throws IOException, LogEntryNotFound {
    if (index.containsKey(seqNum)) {
      return index.get(seqNum);
    } else {
      try (PersistenceReader reader = getReaderAtSeqNum(seqNum)) {
        return reader.position();
      }
    }
  }

  @Override
  public InputStream getStreamAtSeqNum(long seqNum) throws IOException, LogEntryNotFound {
    return Channels.newInputStream(getReaderAtSeqNum(seqNum));
  }

  @Override
  public InputStream getStreamAtFirstEntry() throws IOException {
    PersistenceReader reader = persistence.getReader();
    reader.position(fileOffset);
    return Channels.newInputStream(reader);
  }

  @Override
  public InputStream getStreamAtLastEntry() throws IOException {
    long lastEntrySeqNum = lastIndexedSeqNum();
    long lastEntryAddress = index.get(lastEntrySeqNum);

    PersistenceReader reader = persistence.getReader();
    reader.position(lastEntryAddress);
    InputStream inputStream = Channels.newInputStream(reader);

    try {
      //noinspection InfiniteLoopStatement
      while (true) {
        long entryStartAddress = reader.position();
        lastEntrySeqNum = codec.skipEntryAndReturnSeqNum(inputStream);
        lastEntryAddress = entryStartAddress;
      }
    } catch (EOFException ignore) {
    }

    reader.position(lastEntryAddress);
    addToIndex(lastEntrySeqNum, lastEntryAddress);
    return inputStream;
  }

  private PersistenceReader getReaderAtSeqNum(long seqNum) throws IOException, LogEntryNotFound {
    PersistenceReader reader = persistence.getReader();
    if (index.containsKey(seqNum)) {
      reader.position(index.get(seqNum));
      return reader;
    }

    reader.position(nearestAddressTo(seqNum));
    InputStream inputStream = Channels.newInputStream(reader);

    try {
      while (true) {
        long entryStartAddress = reader.position();
        long entrySeqNum = codec.skipEntryAndReturnSeqNum(inputStream);
        if (seqNum == entrySeqNum) {
          reader.position(entryStartAddress);
          addToIndex(seqNum, entryStartAddress);
          return reader;
        }
      }
    } catch (EOFException e) {
      throw new LogEntryNotFound("EOF reached before finding requested seqNum (" + seqNum + ")");
    }
  }

  /**
   * @return The greatest seqNum in the index, or 0 if no seqNum has ever been added to the index.
   */
  private long lastIndexedSeqNum() {
    return index.lastKey();
  }

  private void maybeAddToIndex(long seqNum, long address) {
    if (seqNum - lastIndexedSeqNum() >= maxEntrySeek) {
      index.put(seqNum, address);
    }
  }

  private long nearestAddressTo(long seqNum) {
    return index.floorEntry(seqNum).getValue();
  }

  private void truncateIndex(long seqNum) {
    index.tailMap(seqNum, true).clear();
  }
}
