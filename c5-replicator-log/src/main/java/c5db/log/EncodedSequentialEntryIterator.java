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
