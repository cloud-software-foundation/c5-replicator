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

import c5db.interfaces.log.Reader;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.util.CheckedSupplier;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

import static c5db.interfaces.log.SequentialEntryIterable.SequentialEntryIterator;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceNavigatorFactory;

/**
 * A Reader for logs beginning with OLogHeader.
 *
 * @param <E> The type of entry this reader returns; it could be different than OLogEntry because some other
 *           codec could be used to decode entries that were initially written as OLogEntry.
 */
public class OLogReader<E extends SequentialEntry> implements Reader<E> {

  private final SequentialEntryCodec<E> codec;
  private final PersistenceNavigatorFactory navigatorFactory = InMemoryPersistenceNavigator::new;
  private final LogPersistenceService<?> logPersistenceService;
  private final String quorumId;

  public OLogReader(SequentialEntryCodec<E> codec,
                    LogPersistenceService<?> logPersistenceService,
                    String quorumId) {
    this.codec = codec;
    this.logPersistenceService = logPersistenceService;
    this.quorumId = quorumId;
  }

  @Override
  public ImmutableList<CheckedSupplier<SequentialEntryIterator<E>, IOException>> getLogList()
  throws IOException {

    ImmutableList.Builder<CheckedSupplier<SequentialEntryIterator<E>, IOException>> logSupplierBuilder =
        ImmutableList.builder();

    for (CheckedSupplier<? extends BytePersistence, IOException> persistenceSupplier :
        logPersistenceService.getList(quorumId)) {

      logSupplierBuilder.add(
          () -> {
            BytePersistence persistence = persistenceSupplier.get();
            PersistenceNavigator navigator =
                SequentialLogWithHeader.createNavigatorFromPersistence(persistence, navigatorFactory, codec);
            return new EncodedSequentialEntryIterator<>(navigator, codec);
          });
    }

    return logSupplierBuilder.build();
  }
}
