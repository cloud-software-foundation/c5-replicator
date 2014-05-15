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
package c5db.regionserver;

import c5db.client.ProtobufUtil;
import c5db.client.generated.Call;
import c5db.client.generated.Get;
import c5db.client.generated.GetRequest;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.Response;
import c5db.interfaces.C5Server;
import c5db.interfaces.TabletModule;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import c5db.tablet.Region;
import c5db.util.C5FiberFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class RegionServerTests {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  Tablet tablet = context.mock(Tablet.class);
  Region region = context.mock(Region.class);


  private final NioEventLoopGroup acceptConnectionGroup = new NioEventLoopGroup(1);
  private final NioEventLoopGroup ioWorkerGroup = new NioEventLoopGroup();
  private final C5FiberFactory c5FiberFactory = context.mock(C5FiberFactory.class);
  private final ChannelHandlerContext ctx = context.mock(ChannelHandlerContext.class);
  private final TabletModule tabletModule = context.mock(TabletModule.class);
  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(Executors.newFixedThreadPool(2));
  private final C5Server server = context.mock(C5Server.class);
  private final Random random = new Random();
  private final int port = 10000 + random.nextInt(100);

  private RegionServerService regionServerService;
  private RegionServerHandler regionServerHandler;


  @Before
  public void before() throws ExecutionException, InterruptedException {
    context.checking(new Expectations() {{
      oneOf(server).getFiberFactory(with(any(Consumer.class)));
      will(returnValue(c5FiberFactory));

      oneOf(c5FiberFactory).create();
      will(returnValue(fiberFactory.create()));
    }});
    regionServerService = new RegionServerService(acceptConnectionGroup, ioWorkerGroup, port, server);

    SettableFuture<TabletModule> tabletModuleSettableFuture = SettableFuture.create();
    tabletModuleSettableFuture.set(tabletModule);
    context.checking(new Expectations() {{
      oneOf(server).getModule(with(any(ModuleType.class)));
      will(returnValue(tabletModuleSettableFuture));
    }});

    ListenableFuture<Service.State> future = regionServerService.start();
    future.get();
    regionServerHandler = new RegionServerHandler(regionServerService);
  }



  @Test(expected = RegionNotFoundException.class)
  public void shouldThrowErrorWhenInvalidRegionSpecifierSpecified() throws Exception {
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME, null);
    Get get = new Get();
    GetRequest getRequest = new GetRequest(regionSpecifier, get);
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldQuietlyHandleInvalidGet() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);
    Get get = new Get();
    GetRequest getRequest = new GetRequest(regionSpecifier, get);

    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));
    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleGetCommandRequestWithNullArgument() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, null, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleMutateWithNullArguments() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MUTATE, 1, null, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleMultiWithNullArgument() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.MULTI, 1, null, null, null, null));
  }


  @Test(expected = IOException.class)
  public void shouldHandleScanCommandRequestWithNullArgument() throws Exception {
    regionServerHandler.channelRead0(ctx, new Call(Call.Command.SCAN, 1, null, null, null, null));
  }



  @Test
  public void shouldBeAbleToHandleGet() throws Exception {
    ByteBuffer regionLocation = ByteBuffer.wrap(Bytes.toBytes("testTable"));
    RegionSpecifier regionSpecifier = new RegionSpecifier(RegionSpecifier.RegionSpecifierType.REGION_NAME,
        regionLocation);


    Get get = ProtobufUtil.toGet(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("fakeRow")), false);
    GetRequest getRequest = new GetRequest(regionSpecifier, get);

    Cell cell = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("value"));
    Result result = Result.create(new Cell[]{cell});
    context.checking(new Expectations() {{
      oneOf(tabletModule).getTablet("testTable");
      will(returnValue(tablet));

      oneOf(tablet).getRegion();
      will(returnValue(region));

      oneOf(region).get(with(any(org.apache.hadoop.hbase.client.Get.class)));
      will(returnValue(result));

      oneOf(ctx).writeAndFlush(with(any(Response.class)));

    }});

    regionServerHandler.channelRead0(ctx, new Call(Call.Command.GET, 1, getRequest, null, null, null));
  }
}
