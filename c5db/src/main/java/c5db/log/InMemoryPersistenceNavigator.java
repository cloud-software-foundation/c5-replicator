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

import io.protostuff.ProtobufException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import static c5db.log.EncodedSequentialLog.Codec;
import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceReader;
import static c5db.log.SequentialLog.LogEntryNotFound;

/**
 * PersistenceNavigator using only in-memory structures, not persisting any data it has been notify()'d.
 */
public class InMemoryPersistenceNavigator<E extends SequentialEntry> implements PersistenceNavigator {

  private final BytePersistence persistence;
  private final Codec<E> codec;

  InMemoryPersistenceNavigator(BytePersistence persistence, Codec<E> codec) {
    this.persistence = persistence;
    this.codec = codec;
  }

  @Override
  public void notify(long seqNum) throws IOException {
    // TODO indexing
  }

  @Override
  public long getAddressOfEntry(long seqNum) throws IOException, LogEntryNotFound {
    try (PersistenceReader reader = getReaderAtSeqNum(seqNum)) {
      return reader.position();
    }
  }

  @Override
  public InputStream getStream(long seqNum) throws IOException, LogEntryNotFound {
    return Channels.newInputStream(getReaderAtSeqNum(seqNum));
  }

  @Override
  public InputStream getStreamAtLastEntry() throws IOException {
    PersistenceReader reader = persistence.getReader();
    InputStream inputStream = Channels.newInputStream(reader);
    long lastEntryAddress = 0;

    // TODO: This is a naive algorithm; apply indexing strategy

    try {
      //noinspection InfiniteLoopStatement
      while (true) {
        long entryStartAddress = reader.position();
        codec.skipEntryAndReturnSeqNum(inputStream);
        lastEntryAddress = entryStartAddress;
      }
    } catch (CrcError | ProtobufException | EOFException ignore) {
      // TODO CrcError or ProtobufException, here as elsewhere, should probably result in a truncation.
      // EOFException -> break loop as intended
    }

    reader.position(lastEntryAddress);
    return inputStream;
  }

  private PersistenceReader getReaderAtSeqNum(long toSeqNum) throws IOException, LogEntryNotFound {
    PersistenceReader reader = persistence.getReader();
    InputStream inputStream = Channels.newInputStream(reader);

    // TODO: This is a naive algorithm; apply indexing strategy
    try {
      while (true) {
        long address = reader.position();
        long seqNum = codec.skipEntryAndReturnSeqNum(inputStream);
        if (toSeqNum == seqNum) {
          reader.position(address);
          return reader;
        }
      }
    } catch (EOFException e) {
      throw new LogEntryNotFound(e);
    }
  }
}
