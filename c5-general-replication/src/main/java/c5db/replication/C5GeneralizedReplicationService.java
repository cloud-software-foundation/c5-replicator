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

    fiber = fiberSupplier.getNewFiber(this::notifyFailed);
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
    Fiber newFiber = fiberSupplier.getNewFiber(throwableConsumer);
    fiber.execute(() -> disposables.add(newFiber));
    newFiber.start();
    return newFiber;
  }

  private void notifyFailed(Throwable throwable) {
    // TODO log and/or propagate the exception
  }
}

