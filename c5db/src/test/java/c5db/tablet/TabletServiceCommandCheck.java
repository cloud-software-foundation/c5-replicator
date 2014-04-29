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
package c5db.tablet;

import c5db.AsyncChannelAsserts;
import c5db.C5ServerConstants;
import c5db.ConfigDirectory;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;
import c5db.util.C5FiberFactory;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.PoolFiberFactoryWithExecutor;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import sun.misc.BASE64Encoder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.TabletMatchers.hasMessageWithState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class TabletServiceCommandCheck {

  private static final String TEST_TABLE_NAME = "testTable";
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};
  private final Channel<Object> newNodeNotificationChannel = new MemoryChannel<>();
  private final SettableFuture stateFuture = SettableFuture.create();
  private final SettableFuture replicationFuture = SettableFuture.create();

  private C5Server c5Server;
  private TabletService tabletService;

  private DiscoveryModule discoveryModule;
  private ReplicationModule replicationModule;
  private ConfigDirectory config;
  private final List<Throwable> throwables = new ArrayList<>();

  private final SettableFuture<DiscoveryModule> discoveryServiceFuture = SettableFuture.create();
  private final SettableFuture<ReplicationModule> replicationServiceFuture = SettableFuture.create();
  private Path configDirectory;
  private Replicator replicator;
  C5FiberFactory fiberFactory = getFiberFactory(this::notifyFailed);
  PoolFiberFactory fiberPool;
  private byte[] tabletDescBytes;
  private byte[] testRegionBytes;

  protected final void notifyFailed(Throwable cause) {
  }


  public C5FiberFactory getFiberFactory(Consumer<Throwable> throwableConsumer) {
    fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
    return new PoolFiberFactoryWithExecutor(fiberPool, new ExceptionHandlingBatchExecutor(throwableConsumer));
  }

  @After
  public void tearDown(){
    fiberPool.dispose();
  }

  @Before
  public void before() throws Exception {

    config = context.mock(ConfigDirectory.class);
    configDirectory = Files.createTempDirectory(null);

    c5Server = context.mock(C5Server.class, "mockC5Server");
    discoveryModule = context.mock(DiscoveryModule.class);
    replicationModule = context.mock(ReplicationModule.class);
    replicator = context.mock(Replicator.class);
    // Reset the underlying fiber


    // Begin to initialize TabletService
    context.checking(new Expectations() {{
      allowing(config).getBaseConfigPath();
      will(returnValue(configDirectory));

      allowing(config).writeBinaryData(with(any(String.class)), with(any(String.class)), with(any(byte[].class)));
      allowing(config).writePeersToFile(with(any(String.class)), with(any(List.class)));
      allowing(config).configuredQuorums();
      will(returnValue(Arrays.asList("testTable,\\x00,1.064e3eb1da827b1dc753e03a797dba37.")));

      oneOf(c5Server).getFiberFactory(with(any(Consumer.class)));
      will(returnValue(fiberFactory));

      // Prepare for the TabletService.doStart
      oneOf(c5Server).getModule(with(ModuleType.Discovery));
      will(returnValue(discoveryServiceFuture));

      // Prepare to set the regionModule for the TabletService
      discoveryServiceFuture.set(discoveryModule);

      oneOf(c5Server).getModule(with(ModuleType.Replication));
      will(returnValue(replicationServiceFuture));

      replicationServiceFuture.set(replicationModule);

      // Begin bootstrap
      oneOf(c5Server).isSingleNodeMode();
      will(returnValue(true));

      oneOf(discoveryModule).getNewNodeNotifications();
      will(returnValue(newNodeNotificationChannel));

      oneOf(discoveryModule).getState();
      will(returnValue(stateFuture));

    }});

    tabletService = new TabletService(c5Server);

  }

  private String createTableString() {
    TableName tableName = TableName.valueOf(Bytes.toBytes(TEST_TABLE_NAME));
    HTableDescriptor testDesc = new HTableDescriptor(tableName);
    testDesc.addFamily(new HColumnDescriptor("testFamily"));
    HRegionInfo testRegion = new HRegionInfo(tableName, new byte[]{0}, new byte[]{}, false, 1);
    String peerString = "1, 2, 3";
    BASE64Encoder encoder = new BASE64Encoder();
    tabletDescBytes = testDesc.toByteArray();
    String hTableDesc = encoder.encodeBuffer(tabletDescBytes);

    testRegionBytes = testRegion.toByteArray();
    String hRegionInfo = encoder.encodeBuffer(testRegionBytes);

    return C5ServerConstants.CREATE_TABLE + ":" + hTableDesc + "," + hRegionInfo + "," + peerString;
  }

  @Test
  public void testCreateTable() throws Throwable {
    context.checking(new Expectations() {
      {
        oneOf(c5Server).getConfigDirectory();
        will(returnValue(config));
      }
    });
    replicationFuture.set(replicator);

    // Prepare the config directory
    ListenableFuture<Service.State> future = tabletService.start();
    future.get();

    Channel channel = new MemoryChannel();
    context.checking(new Expectations() {
      {
        oneOf(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
        will(returnValue(replicationFuture));

        oneOf(replicator).getStateChannel();
        will(returnValue(channel));

        oneOf(replicator).start();
        oneOf(replicator).getQuorumId();
        will(returnValue("1"));
      }
    });

    tabletService.acceptCommand(createTableString());

    context.checking(new Expectations() {
      {
        oneOf(replicationModule).createReplicator(with(any(String.class)), with(any(List.class)));
        will(returnValue(replicationFuture));

        oneOf(replicator).getStateChannel();
        will(returnValue(channel));

        oneOf(replicator).start();
        oneOf(replicator).getQuorumId();
        will(returnValue("1"));

        allowing(config).writeBinaryData(with(any(String.class)), with(any(String.class)), with(any(byte[].class)));

        oneOf(config).readBinaryData(with(any(String.class)), with(any(String.class)));
        will(returnValue(testRegionBytes));

        oneOf(config).readBinaryData(with(any(String.class)), with(any(String.class)));
        will(returnValue(tabletDescBytes));

        oneOf(config).readPeers(with(any(String.class)));
        will(returnValue(Arrays.asList(1l)));


        allowing(config).configuredQuorums();

      }
    });

    TabletRegistry tabletRegistry = new TabletRegistry(c5Server,
        config,
        HBaseConfiguration.create(),
        fiberFactory,
        replicationModule,
        ReplicatedTablet::new,
        HRegionBridge::new);

    tabletRegistry.startOnDiskRegions();
    Map<String, c5db.interfaces.tablet.Tablet> tablets = tabletRegistry.getTablets();
    assertThat(tablets.size(), is(equalTo(1)));
    assertThat(tablets.keySet().iterator().next(), startsWith(TEST_TABLE_NAME));

    c5db.interfaces.tablet.Tablet singleTablet = tablets.values().iterator().next();
    AsyncChannelAsserts.ChannelListener<TabletStateChange> listener
        = listenTo(singleTablet.getStateChangeChannel());

    c5db.interfaces.tablet.Tablet.State state = singleTablet.getTabletState();
    if (!state.equals(c5db.interfaces.tablet.Tablet.State.Open)) {
      assertEventually(listener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));
    }
  }
}
