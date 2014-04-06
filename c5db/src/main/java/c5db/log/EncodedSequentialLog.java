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

import com.google.common.collect.Lists;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.LogPersistenceService.PersistenceNavigator;

/**
 * Sequential log that encodes and decodes its entries to bytes, persisting them to a BytePersistence.
 */
public class EncodedSequentialLog<E extends SequentialEntry> implements SequentialLog<E> {
  private final LogPersistenceService.BytePersistence persistence;
  private final Codec<E> codec;
  private final PersistenceNavigator persistenceNavigator;

  /**
   * Encapsulates capability to serialize and deserialize log entry objects.
   *
   * @param <E>
   */
  public interface Codec<E extends SequentialEntry> {
    /**
     * Serialize an entry, including prepending any length necessary to reconstruct
     * the entry, and including any necessary CRCs.
     *
     * @param entry An entry to be serialized.
     * @return An array of ByteBuffer containing the serialized data.
     */
    ByteBuffer[] encode(E entry);

    /**
     * Deserialize an entry from an input stream, and check its CRC.
     *
     * @param inputStream An open input stream, positioned at the start of an entry.
     * @return The reconstructed entry.
     * @throws CrcError
     * @throws IOException
     */
    E decode(InputStream inputStream) throws IOException, CrcError;

    /**
     * Skip over an entry in the input stream, returning the sequence number of the entry
     * encountered.
     *
     * @param inputStream An open input stream, positioned at the start of an entry.
     * @return The sequence number of the entry encountered.
     * @throws CrcError
     * @throws IOException
     */
    long skipEntryAndReturnSeqNum(InputStream inputStream) throws IOException, CrcError;
  }

  public EncodedSequentialLog(LogPersistenceService.BytePersistence persistence,
                              Codec<E> codec,
                              PersistenceNavigator persistenceNavigator) {
    this.persistence = persistence;
    this.codec = codec;
    this.persistenceNavigator = persistenceNavigator;
  }

  @Override
  public void append(List<E> entries) throws IOException {
    for (E entry : entries) {
      persistenceNavigator.notify(entry.getSeqNum());
      persistence.append(codec.encode(entry));
    }
  }

  @Override
  public List<E> subSequence(long start, long end) throws IOException {
    final List<E> readEntries = Lists.newArrayList();

    if (end < start) {
      throw new IllegalArgumentException("subSequence: end < start");
    } else if (end == start) {
      return readEntries;
    }

    try (InputStream reader = persistenceNavigator.getStream(start)) {
      while (true) {
        E entry = codec.decode(reader);

        if (entry.getSeqNum() < end) {
          ensureAscendingWithNoGaps(readEntries, entry);
          readEntries.add(entry);
        } else {
          break;
        }
      }
    } catch (EOFException ignore) {
    }

    return readEntries;
  }

  @Override
  public void truncate(long seqNum) throws IOException {
    long truncationPos = persistenceNavigator.getAddressOfEntry(seqNum);
    persistence.truncate(truncationPos);
  }

  @Override
  public void sync() throws IOException {
    persistence.sync();
  }

  @Override
  public void close() throws IOException {
    persistence.close();
  }

  /**
   * Exception indicating a log entry has been read with an incorrect sequence number.
   */
  public static class LogEntryNotInSequence extends RuntimeException {
    public LogEntryNotInSequence() {
      super();
    }
  }

  private void ensureAscendingWithNoGaps(List<E> entries, E entry) throws LogEntryNotInSequence {
    final int size = entries.size();
    if (size > 0) {
      final E lastEntry = entries.get(size - 1);
      if (lastEntry.getSeqNum() + 1 != entry.getSeqNum()) {
        throw new LogEntryNotInSequence();
      }
    }
  }
}
