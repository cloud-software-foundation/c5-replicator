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

import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static c5db.interfaces.log.SequentialEntryIterable.SequentialEntryIterator;
import static c5db.log.LogPersistenceService.PersistenceNavigator;

/**
 * Implementation of SequentialEntryIterator for logs encoded with a SequentialEntryCodec.
 */
class EncodedSequentialEntryIterator<E extends SequentialEntry> implements SequentialEntryIterator<E> {
  private final SequentialEntryCodec<E> codec;
  private final InputStream inputStream;
  private E nextEntry;

  EncodedSequentialEntryIterator(PersistenceNavigator persistenceNavigator, SequentialEntryCodec<E> codec)
      throws IOException {

    this.codec = codec;
    this.inputStream = persistenceNavigator.getStreamAtFirstEntry();
    this.nextEntry = fetchNext();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public boolean hasNext() throws IOException {
    return (nextEntry != null);
  }

  @Override
  public E next() throws IOException {
    E thisEntry = nextEntry;
    nextEntry = fetchNext();
    return thisEntry;
  }

  private E fetchNext() throws IOException {
    try {
      return codec.decode(inputStream);
    } catch (EOFException e) {
      return null;
    }
  }
}
