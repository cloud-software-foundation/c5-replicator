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
 *            codec could be used to decode entries that were initially written as OLogEntry.
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
