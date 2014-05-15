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

import c5db.client.C5AsyncDatabase;
import c5db.client.FakeHTable;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.ControlModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ManyClusterBase {
  private static int regionServerPort;

  private static final Random rnd = new Random();

  private static Channel<TabletStateChange> stateChanges;
  private static Channel<TabletStateChange> stateChanges1;
  private static Channel<TabletStateChange> stateChanges2;
  private static Channel<CommandRpcRequest<?>> commandChannel;
  public static final byte[] value = Bytes.toBytes("value");


  @Rule
  public TestName name = new TestName();
  public FakeHTable table;
  public static int metaOnPort;
  public byte[] row;
  private int userTabletOn;

  public int getRegionServerPort() {
    Log.info("Getting region from: " + userTabletOn);
    return userTabletOn;
  }

  private static C5Server server;
  private static C5Server server1;
  private static C5Server server2;

  String getCreateTabletSubCommand(ByteString tableNameBytes) {
    TableName tableName = TableName.valueOf(tableNameBytes.toByteArray());
    HTableDescriptor testDesc = new HTableDescriptor(tableName);
    testDesc.addFamily(new HColumnDescriptor("cf"));
    HRegionInfo testRegion = new HRegionInfo(tableName, new byte[]{0}, new byte[]{}, false, 1);
    String peerString = String.valueOf(server.getNodeId() + "," + server1.getNodeId() + "," + server2.getNodeId());
    BASE64Encoder encoder = new BASE64Encoder();

    String hTableDesc = encoder.encodeBuffer(testDesc.toByteArray());
    String hRegionInfo = encoder.encodeBuffer(testRegion.toByteArray());

    return C5ServerConstants.CREATE_TABLE + ":" + hTableDesc + "," + hRegionInfo + "," + peerString;

  }

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Fiber receiver = new ThreadFiber();
    receiver.start();

    final CountDownLatch latch = new CountDownLatch(1);

    Callback<TabletStateChange> onMsg = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        userTabletOn = regionServerPort - 2;
        latch.countDown();
      }
    };
    Callback<TabletStateChange> onMsg1 = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        userTabletOn = regionServerPort - 1;
        latch.countDown();
      }
    };
    Callback<TabletStateChange> onMsg2 = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        userTabletOn = regionServerPort;
        latch.countDown();
      }
    };
    stateChanges.subscribe(receiver, onMsg);
    stateChanges1.subscribe(receiver, onMsg1);
    stateChanges2.subscribe(receiver, onMsg2);

    final ByteString tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));
    ModuleSubCommand createTableSubCommand = new ModuleSubCommand(ModuleType.Tablet,
        getCreateTabletSubCommand(tableName));

    commandChannel.publish(new CommandRpcRequest<>(server.getNodeId(), createTableSubCommand));
    commandChannel.publish(new CommandRpcRequest<>(server1.getNodeId(), createTableSubCommand));
    commandChannel.publish(new CommandRpcRequest<>(server2.getNodeId(), createTableSubCommand));
    // create java.util.concurrent.CountDownLatch to notify when message arrives
    latch.await();

    table = new FakeHTable(C5TestServerConstants.LOCALHOST, userTabletOn, tableName);
    row = Bytes.toBytes(name.getMethodName());

    receiver.dispose();
  }

  @After
  public void after() throws InterruptedException {
    table.close();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, ExecutionException, TimeoutException {
    List<ListenableFuture<Service.State>> states;

    states = new ArrayList<>();
    for (C5Module module : server.getModules().values()) {
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

    states = new ArrayList<>();
    for (C5Module module : server1.getModules().values()) {
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
    server1.stopAndWait();

    for (C5Module module : server2.getModules().values()) {
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
    server2.stopAndWait();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Thread.sleep(1000);
    Log.warn("-----------------------------------------------------------------------------------------------------------");
    System.setProperty("clusterName", String.valueOf("foo"));

    regionServerPort = 8080 + rnd.nextInt(1000);
    int webServerPort = 31337 + rnd.nextInt(1000);
    int controlServerPort = C5ServerConstants.CONTROL_RPC_PROPERTY_PORT + rnd.nextInt(100);
    System.setProperty(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME, String.valueOf(regionServerPort));
    System.setProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME, String.valueOf(webServerPort));
    System.setProperty(C5ServerConstants.CONTROL_SERVER_PORT_PROPERTY_NAME, String.valueOf(controlServerPort));
    server = Main.startC5Server(new String[]{});

    System.setProperty(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME, String.valueOf(++regionServerPort));
    System.setProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME, String.valueOf(++webServerPort));
    System.setProperty(C5ServerConstants.CONTROL_SERVER_PORT_PROPERTY_NAME, String.valueOf(++controlServerPort));
    server1 = Main.startC5Server(new String[]{});

    System.setProperty(C5ServerConstants.REGION_SERVER_PORT_PROPERTY_NAME, String.valueOf(++regionServerPort));
    System.setProperty(C5ServerConstants.WEB_SERVER_PORT_PROPERTY_NAME, String.valueOf(++webServerPort));
    System.setProperty(C5ServerConstants.CONTROL_SERVER_PORT_PROPERTY_NAME, String.valueOf(++controlServerPort));
    server2 = Main.startC5Server(new String[]{});

    ListenableFuture<C5Module> regionServerFuture = server.getModule(ModuleType.RegionServer);
    ListenableFuture<C5Module> tabletServerFuture = server.getModule(ModuleType.Tablet);
    ListenableFuture<C5Module> replicationServerFuture = server.getModule(ModuleType.Replication);
    ListenableFuture<C5Module> controlServerFuture = server.getModule(ModuleType.ControlRpc);

    C5Module regionServer = regionServerFuture.get();
    TabletModule tabletServer = (TabletModule) tabletServerFuture.get();
    ReplicationModule replicationServer = (ReplicationModule) replicationServerFuture.get();
    ControlModule controlServer = (ControlModule) controlServerFuture.get();

    while (!regionServer.isRunning()
        || !tabletServer.isRunning()
        || !replicationServer.isRunning()
        || !controlServer.isRunning()) {
      Thread.sleep(600);
    }

    ListenableFuture<C5Module> regionServerFuture1 = server1.getModule(ModuleType.RegionServer);
    ListenableFuture<C5Module> tabletServerFuture1 = server1.getModule(ModuleType.Tablet);
    ListenableFuture<C5Module> replicationServerFuture1 = server1.getModule(ModuleType.Replication);
    ListenableFuture<C5Module> controlServerFuture1 = server1.getModule(ModuleType.ControlRpc);

    C5Module regionServer1 = regionServerFuture1.get();
    TabletModule tabletServer1 = (TabletModule) tabletServerFuture1.get();
    ReplicationModule replicationServer1 = (ReplicationModule) replicationServerFuture1.get();
    ControlModule controlServer1 = (ControlModule) controlServerFuture1.get();

    while (!regionServer1.isRunning()
        || !tabletServer1.isRunning()
        || !replicationServer1.isRunning()
        || !controlServer1.isRunning()) {
      Thread.sleep(600);
    }

    ListenableFuture<C5Module> regionServerFuture2 = server2.getModule(ModuleType.RegionServer);
    ListenableFuture<C5Module> tabletServerFuture2 = server2.getModule(ModuleType.Tablet);
    ListenableFuture<C5Module> replicationServerFuture2 = server2.getModule(ModuleType.Replication);
    ListenableFuture<C5Module> controlServerFuture2 = server2.getModule(ModuleType.ControlRpc);

    C5Module regionServer2 = regionServerFuture2.get();
    TabletModule tabletServer2 = (TabletModule) tabletServerFuture2.get();
    ReplicationModule replicationServer2 = (ReplicationModule) replicationServerFuture2.get();
    ControlModule controlServer2 = (ControlModule) controlServerFuture2.get();

    while (!regionServer2.isRunning()
        || !tabletServer2.isRunning()
        || !replicationServer2.isRunning()
        || !controlServer2.isRunning()) {
      Thread.sleep(600);
    }

    stateChanges = tabletServer.getTabletStateChanges();
    stateChanges1 = tabletServer1.getTabletStateChanges();
    stateChanges2 = tabletServer2.getTabletStateChanges();

    Fiber receiver = new ThreadFiber();
    receiver.start();

    // create java.util.concurrent.CountDownLatch to notify when message arrives
    final CountDownLatch latch = new CountDownLatch(2);

    Callback<TabletStateChange> onMsg1 = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        System.out.println("Found: " + message.tablet.getRegionInfo().getRegionNameAsString());
        if (message.tablet.getRegionInfo().getRegionNameAsString().startsWith("hbase:meta")) {
          metaOnPort = regionServerPort - 2;
          commandChannel = server.getCommandChannel();
        }
        latch.countDown();
      }
    };

    Callback<TabletStateChange> onMsg2 = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        System.out.println("Found: " + message.tablet.getRegionInfo().getRegionNameAsString());
        if (message.tablet.getRegionInfo().getRegionNameAsString().startsWith("hbase:meta")) {
          metaOnPort = regionServerPort - 1;
          commandChannel = server1.getCommandChannel();
        }
        latch.countDown();
      }
    };

    Callback<TabletStateChange> onMsg3 = message -> {
      System.out.println(message);
      if (message.state.equals(Tablet.State.Leader)) {
        System.out.println("Found: " + message.tablet.getRegionInfo().getRegionNameAsString());
        if (message.tablet.getRegionInfo().getRegionNameAsString().startsWith("hbase:meta")) {
          metaOnPort = regionServerPort;
          commandChannel = server2.getCommandChannel();
        }
        latch.countDown();
      }
    };

    stateChanges.subscribe(receiver, onMsg1);
    stateChanges1.subscribe(receiver, onMsg2);
    stateChanges2.subscribe(receiver, onMsg3);

    latch.await();
    receiver.dispose();

  }

}