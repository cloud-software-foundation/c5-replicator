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

import c5db.SimpleC5ModuleServer;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.GeneralizedReplicationService;
import c5db.interfaces.LogModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.log.Reader;
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorEntry;
import c5db.log.LogService;
import c5db.log.OLogToReplicatorEntryCodec;
import c5db.util.C5Futures;
import c5db.util.FiberSupplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.allAsList;


/**
 * Replication server implementing a variation of the Raft algorithm; internally
 * it bundles a ReplicationModule, a LogModule (based on OLog), and a DiscoveryModule
 * (passed in as a constructor argument).
 */
public class C5GeneralizedReplicationService extends AbstractService implements GeneralizedReplicationService {
  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();

  private final FiberSupplier fiberSupplier;
  private final Fiber serverFiber;
  private final SimpleC5ModuleServer moduleServer;
  private final DiscoveryModule nodeInfoModule;
  private final LogModule logModule;
  private final ReplicationModule replicationModule;

  private final List<Disposable> disposables = new ArrayList<>();

  private final EventLoopGroup bossGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);

  public C5GeneralizedReplicationService(
      Path basePath,
      long nodeId,
      int replicatorPort,
      DiscoveryModule nodeInfoModule,
      FiberSupplier fiberSupplier) {

    this.nodeInfoModule = nodeInfoModule;
    this.fiberSupplier = fiberSupplier;

    serverFiber = fiberSupplier.getFiber(this::notifyFailed);
    moduleServer = new SimpleC5ModuleServer(serverFiber);
    serverFiber.start();

    replicationModule = new ReplicatorService(bossGroup, workerGroup, nodeId, replicatorPort, moduleServer,
        fiberSupplier, new NioQuorumFileReaderWriter(basePath));

    logModule = new LogService(basePath, fiberSupplier);
  }

  @Override
  protected void doStart() {
    List<ListenableFuture<Service.State>> startFutures = new ArrayList<>();

    startFutures.add(moduleServer.startModule(logModule));
    startFutures.add(moduleServer.startModule(nodeInfoModule));
    startFutures.add(moduleServer.startModule(replicationModule));

    ListenableFuture<List<Service.State>> allFutures = allAsList(startFutures);

    C5Futures.addCallback(allFutures,
        (List<Service.State> ignore) -> notifyStarted(),
        this::notifyFailed,
        serverFiber);
  }

  @Override
  protected void doStop() {
    replicationModule.stopAndWait();
    nodeInfoModule.stopAndWait();
    logModule.stopAndWait();

    notifyStopped();
  }

  @Override
  public ListenableFuture<GeneralizedReplicator> createReplicator(String quorumId, Collection<Long> peerIds) {
    return Futures.transform(
        replicationModule.createReplicator(quorumId, peerIds),
        (Replicator replicator) -> {
          replicator.start();
          // TODO the failure of the fiber passed to C5GeneralizedReplicator should not fail this entire service.
          return new C5GeneralizedReplicator(replicator, createAndStartFiber(this::notifyFailed));
        });
  }

  @Override
  public Reader<ReplicatorEntry> getLogReader(String quorumId) {
    return logModule.getLogReader(quorumId, new OLogToReplicatorEntryCodec());
  }

  public void dispose() {
    serverFiber.dispose();
    disposables.forEach(Disposable::dispose);
  }

  private Fiber createAndStartFiber(Consumer<Throwable> throwableHandler) {
    Fiber newFiber = fiberSupplier.getFiber(throwableHandler);
    serverFiber.execute(() -> disposables.add(newFiber));
    newFiber.start();
    return newFiber;
  }
}

