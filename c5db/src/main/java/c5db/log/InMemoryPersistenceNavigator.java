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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import static c5db.log.EncodedSequentialLog.Codec;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceReader;

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
  public long getAddressOfEntry(long seqNum) throws IOException {
    try (PersistenceReader reader = getReaderAtSeqNum(seqNum)) {
      return reader.position();
    }
  }

  @Override
  public InputStream getStream(long seqNum) throws IOException {
    return Channels.newInputStream(getReaderAtSeqNum(seqNum));
  }

  private PersistenceReader getReaderAtSeqNum(long toSeqNum) throws IOException {
    PersistenceReader reader = persistence.getReader();
    InputStream inputStream = Channels.newInputStream(reader);

    // TODO apply indexing information

    while (true) {
      long address = reader.position();
      long seqNum = codec.skipEntryAndReturnSeqNum(inputStream);
      if (toSeqNum == seqNum) {
        reader.position(address);
        return reader;
      }
    }
  }
}
