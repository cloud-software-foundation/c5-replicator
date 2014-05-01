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
package c5db;

import c5db.client.C5Table;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Channel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MiniClusterBase {
  private static int regionServerPort;
  public static final byte[] value = Bytes.toBytes("value");
  public static final byte[] notEqualToValue = Bytes.toBytes("notEqualToValue");
  private static final Random rnd = new Random();
  private static Channel<TabletStateChange> stateChanges;

  @Rule
  public TestName name = new TestName();
  public C5Table table;
  public byte[] row;

  public static int getRegionServerPort() {
    return regionServerPort;
  }

  private static C5Server server;

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Fiber receiver = new ThreadFiber();
    receiver.start();

    final CountDownLatch latch = new CountDownLatch(1);

    Callback<TabletStateChange> onMsg = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Open)
          || message.state.equals(Tablet.State.Leader)) {
        latch.countDown();
      }
    };
    stateChanges.subscribe(receiver, onMsg);


    final ByteString tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));
    Channel<CommandRpcRequest<?>> commandChannel = server.getCommandChannel();

    ModuleSubCommand createTableSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        TestHelpers.getCreateTabletSubCommand(tableName, server.getNodeId()));
    CommandRpcRequest<ModuleSubCommand> createTableCommand = new CommandRpcRequest<>(server.getNodeId(),
        createTableSubCommand);

    commandChannel.publish(createTableCommand);
    // create java.util.concurrent.CountDownLatch to notify when message arrives
    latch.await();

    table = new C5Table(tableName, getRegionServerPort());
    row = Bytes.toBytes(name.getMethodName());
    receiver.dispose();
  }

  @After
  public void after() {
    table.close();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, ExecutionException, TimeoutException {

    ImmutableMap<ModuleType, C5Module> modules = server.getModules();

    List<ListenableFuture<Service.State>> states = new ArrayList<>();
    for (C5Module module : modules.values()) {
      ListenableFuture<Service.State> future = module.stop();
      states.add(future);
    }

    for (ListenableFuture<Service.State> state : states) {
      try {
        state.get(10000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    server.stopAndWait();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Log.warn("-----------------------------------------------------------------------------------------------------------");


    regionServerPort = 8080 + rnd.nextInt(1000);

    System.setProperty("regionServerPort", String.valueOf(regionServerPort));

    server = Main.startC5Server(new String[]{});

    ListenableFuture<C5Module> regionServerFuture = server.getModule(ModuleType.RegionServer);
    C5Module regionServer = regionServerFuture.get();

    ListenableFuture<C5Module> tabletServerFuture = server.getModule(ModuleType.Tablet);
    TabletModule tabletServer = (TabletModule) tabletServerFuture.get();

    ListenableFuture<C5Module> replicationServerFuture = server.getModule(ModuleType.Replication);
    ReplicationModule replicationServer = (ReplicationModule) replicationServerFuture.get();

    stateChanges = tabletServer.getTabletStateChanges();

    Fiber receiver = new ThreadFiber();
    receiver.start();

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    final CountDownLatch latch = new CountDownLatch(2);

    Callback<TabletStateChange> onMsg = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        latch.countDown();
      }
    };
    stateChanges.subscribe(receiver, onMsg);

    while (!regionServer.isRunning() || !tabletServer.isRunning() || !replicationServer.isRunning()) {
      Thread.sleep(600);
    }

    latch.await();
    receiver.dispose();
  }
}