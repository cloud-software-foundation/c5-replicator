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

package c5db.replication;

import c5db.interfaces.GeneralizedReplicationService;
import c5db.interfaces.LogModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.log.Reader;
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorEntry;
import c5db.log.OLogToReplicatorEntryCodec;
import c5db.util.FiberSupplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;


/**
 * A GeneralizedReplicationService implementing a variation of the Raft algorithm; internally
 * it bundles a ReplicationModule and a LogModule (based on OLog). This class just wraps
 * the modules, and doesn't do any lifecycle management; it assumes they are started elsewhere
 * and have finished starting up before any of the GeneralizedReplicationService methods are called.
 */
public class C5GeneralizedReplicationService implements GeneralizedReplicationService {

  private final FiberSupplier fiberSupplier;
  private final Fiber fiber;
  private final LogModule logModule;
  private final ReplicationModule replicationModule;

  private final List<Disposable> disposables = new ArrayList<>();

  public C5GeneralizedReplicationService(
      ReplicationModule replicationModule,
      LogModule logModule,
      FiberSupplier fiberSupplier) {

    this.replicationModule = replicationModule;
    this.logModule = logModule;
    this.fiberSupplier = fiberSupplier;

    fiber = fiberSupplier.getFiber(this::notifyFailed);
    fiber.start();
  }

  @Override
  public ListenableFuture<GeneralizedReplicator> createReplicator(String quorumId, Collection<Long> peerIds) {
    return Futures.transform(
        replicationModule.createReplicator(quorumId, peerIds),
        (Replicator replicator) -> {
          return new C5GeneralizedReplicator(replicator, createAndStartFiber(this::notifyFailed));
        });
  }

  @Override
  public Reader<ReplicatorEntry> getLogReader(String quorumId) {
    return logModule.getLogReader(quorumId, new OLogToReplicatorEntryCodec());
  }

  public void dispose() {
    fiber.dispose();
    disposables.forEach(Disposable::dispose);
  }

  private Fiber createAndStartFiber(Consumer<Throwable> throwableConsumer) {
    Fiber newFiber = fiberSupplier.getFiber(throwableConsumer);
    fiber.execute(() -> disposables.add(newFiber));
    newFiber.start();
    return newFiber;
  }

  private void notifyFailed(Throwable throwable) {
    // TODO log and/or propagate the exception
  }
}

