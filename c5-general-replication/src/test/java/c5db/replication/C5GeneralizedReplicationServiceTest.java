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

import c5db.C5CommonTestUtil;
import c5db.SimpleC5ModuleServer;
import c5db.discovery.BeaconService;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.LogModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.log.LogService;
import c5db.log.ReplicatorLogGenericTestUtil;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.hamcrest.Matcher;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static c5db.CollectionMatchers.isStrictlyIncreasing;
import static c5db.FutureMatchers.resultsIn;
import static c5db.ReplicatorConstants.REPLICATOR_PORT_MIN;
import static com.google.common.util.concurrent.Futures.allAsList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class C5GeneralizedReplicationServiceTest {
  @Rule
  public JUnitRuleFiberExceptions jUnitFiberExceptionHandler = new JUnitRuleFiberExceptions();

  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();
  private static final int DISCOVERY_PORT = 54333;

  private final Path baseTestPath = new C5CommonTestUtil().getDataTestDir("general-replicator-test");

  private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_PROCESSORS);
  private final EventLoopGroup bossGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);

  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(executorService);
  private final Set<Fiber> fibers = new HashSet<>();
  private final Fiber mainTestFiber = newExceptionHandlingFiber(jUnitFiberExceptionHandler);

  @Before
  public void setupConfigDirectory() throws Exception {
    mainTestFiber.start();
  }

  @After
  public void disposeOfResources() throws Exception {
    fiberFactory.dispose();
    executorService.shutdownNow();
    fibers.forEach(Fiber::dispose);

    // Initiate shut down but don't wait for termination, for the sake of test speed.
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  @Test(timeout = 9000)
  public void logsToASingleQuorumReplicatorUsingTheGeneralizedInterface() throws Exception {
    long nodeId = 1;
    List<Long> peerIds = Lists.newArrayList(1L);

    try (SingleQuorumReplicationServer serverFixture
             = new SingleQuorumReplicationServer(nodeId, peerIds, this::newExceptionHandlingFiber)) {

      GeneralizedReplicator replicator = serverFixture.replicator;

      waitUntilReplicatorIsAvailable(replicator);

      List<ListenableFuture<Long>> replicateFutures = new ArrayList<ListenableFuture<Long>>() {{
        add(replicator.replicate(someData()));
        add(replicator.replicate(someData()));
        add(replicator.replicate(someData()));
      }};

      assertThat(allAsList(replicateFutures), resultsInAListOfLongsThat(hasSize(3)));
      assertThat(allAsList(replicateFutures), resultsInAListOfLongsThat(isStrictlyIncreasing()));
    }
  }

  private void waitUntilReplicatorIsAvailable(GeneralizedReplicator replicator) throws Exception {
    replicator.isAvailableFuture().get();
  }

  private Fiber newExceptionHandlingFiber(Consumer<Throwable> throwableHandler) {
    Fiber newFiber = fiberFactory.create(new ExceptionHandlingBatchExecutor(throwableHandler));
    fibers.add(newFiber);
    return newFiber;
  }

  private List<ByteBuffer> someData() {
    return Lists.newArrayList(ReplicatorLogGenericTestUtil.someData());
  }

  private static Matcher<? super ListenableFuture<List<Long>>> resultsInAListOfLongsThat(
      Matcher<? super List<Long>> longsMatcher) {
    return resultsIn(longsMatcher);
  }

  /**
   * Runs a C5GeneralizedReplicationService and handles startup and disposal,
   * for the purpose of making tests more readable
   */
  private class SingleQuorumReplicationServer implements AutoCloseable {
    private static final String QUORUM_ID = "quorumId";

    public final C5GeneralizedReplicationService service;
    public final GeneralizedReplicator replicator;

    private final SimpleC5ModuleServer moduleServer;
    private final ReplicationModule replicationModule;
    private final LogModule logModule;
    private final DiscoveryModule nodeInfoModule;

    public SingleQuorumReplicationServer(long nodeId, Collection<Long> peerIds, FiberSupplier fiberSupplier)
        throws Exception {

      moduleServer = new SimpleC5ModuleServer(mainTestFiber, jUnitFiberExceptionHandler);

      replicationModule =
          new ReplicatorService(bossGroup, workerGroup, nodeId, REPLICATOR_PORT_MIN, moduleServer, fiberSupplier,
              new NioQuorumFileReaderWriter(baseTestPath));
      logModule = new LogService(baseTestPath, fiberSupplier);
      nodeInfoModule = new BeaconService(nodeId, DISCOVERY_PORT, workerGroup, moduleServer, fiberSupplier);

      startAll();

      service = new C5GeneralizedReplicationService(replicationModule, logModule, fiberSupplier);
      replicator = service.createReplicator(QUORUM_ID, peerIds).get();
    }

    @Override
    public void close() {
      service.dispose();
      stopAll();
    }

    private void startAll() throws Exception {
      List<ListenableFuture<Service.State>> startFutures = new ArrayList<>();

      startFutures.add(moduleServer.startModule(logModule));
      startFutures.add(moduleServer.startModule(nodeInfoModule));
      startFutures.add(moduleServer.startModule(replicationModule));

      ListenableFuture<List<Service.State>> allFutures = allAsList(startFutures);

      // Block waiting for everything to start.
      allFutures.get();
    }

    private void stopAll() {
      replicationModule.stopAndWait();
      nodeInfoModule.stopAndWait();
      logModule.stopAndWait();
    }
  }
}
