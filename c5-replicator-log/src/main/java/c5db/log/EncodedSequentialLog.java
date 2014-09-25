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
import c5db.interfaces.log.SequentialEntryCodec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;

/**
 * Sequential log that encodes and decodes its entries to bytes, persisting them to a BytePersistence.
 */
public class EncodedSequentialLog<E extends SequentialEntry> implements SequentialLog<E> {
  private final BytePersistence persistence;
  private final SequentialEntryCodec<E> codec;
  private final PersistenceNavigator persistenceNavigator;

  public EncodedSequentialLog(BytePersistence persistence,
                              SequentialEntryCodec<E> codec,
                              PersistenceNavigator persistenceNavigator) {
    this.persistence = persistence;
    this.codec = codec;
    this.persistenceNavigator = persistenceNavigator;
  }

  @Override
  public void append(List<E> entries) throws IOException {
    for (E entry : entries) {
      persistenceNavigator.notifyLogging(entry.getSeqNum(), persistence.size());
      persistence.append(codec.encode(entry));
    }
  }

  @Override
  public List<E> subSequence(long start, long end) throws IOException, LogEntryNotFound, LogEntryNotInSequence {
    final List<E> readEntries = new ArrayList<>();

    try (InputStream reader = persistenceNavigator.getStreamAtSeqNum(start)) {
      long seqNum;
      do {
        E entry = codec.decode(reader);
        readEntries.add(entry);
        seqNum = entry.getSeqNum();
      } while (seqNum < end - 1);
    } catch (EOFException e) {
      throw new LogEntryNotFound("EOF reached before finding all requested entries: seqNum range ["
          + start + ", " + end + ")");
    }

    ensureAscendingWithNoGaps(readEntries);
    return readEntries;
  }

  @Override
  public boolean isEmpty() throws IOException {
    return persistence.isEmpty();
  }

  @Override
  public E getLastEntry() throws IOException {
    if (isEmpty()) {
      return null;
    }

    try (InputStream inputStream = persistenceNavigator.getStreamAtLastEntry()) {
      return codec.decode(inputStream);
    }
  }

  @Override
  public SequentialEntryIterator<E> iterator() throws IOException {
    return new EncodedSequentialEntryIterator<>(persistenceNavigator, codec);
  }

  @Override
  public void truncate(long seqNum) throws IOException, LogEntryNotFound {
    long truncationPos = persistenceNavigator.getAddressOfEntry(seqNum);
    persistence.truncate(truncationPos);
    persistenceNavigator.notifyTruncation(seqNum);
  }

  @Override
  public void sync() throws IOException {
    persistence.sync();
  }

  @Override
  public void close() throws IOException {
    persistence.close();
  }

  private void ensureAscendingWithNoGaps(List<E> entries) throws LogEntryNotInSequence {
    final int size = entries.size();
    if (size > 0) {
      for (int i = 1; i < size; i++) {
        if (entries.get(i).getSeqNum() != entries.get(i - 1).getSeqNum() + 1) {
          throw new LogEntryNotInSequence();
        }
      }
    }
  }

}
