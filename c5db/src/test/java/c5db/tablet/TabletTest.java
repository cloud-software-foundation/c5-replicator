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

import c5db.interfaces.ReplicationModule;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertTrue;


/**
 *
 */
public class TabletTest {
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  //HRegion region = context.mock(HRegion.class);
  ReplicationModule.Replicator replicator = context.mock(ReplicationModule.Replicator.class);
  IRegion.Creator regionCreator = context.mock(IRegion.Creator.class);
  IRegion region = context.mock(IRegion.class);
  HLog log = context.mock(HLog.class);

  final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);
  final HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf("tablename"));
  final String regionName = regionInfo.getRegionNameAsString();
  final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("tablename"));
  // TODO real path.
  final Path path = Paths.get("/");
  final Configuration conf = new Configuration();


  @Test
  public void basicTest() throws Exception {
    SettableFuture<ReplicationModule.Replicator> future = SettableFuture.create();
    future.set(replicator);

    context.checking(new Expectations() {{

      oneOf(replicationModule).createReplicator(regionName, peerList);
      will(returnValue(future));

      oneOf(replicator).start();

      oneOf(regionCreator).getHRegion(path, regionInfo, tableDescriptor, log, conf);
      will(returnValue(region));
    }});

    Fiber f = new ThreadFiber();
    Tablet tablet = new Tablet(
        regionInfo,
        tableDescriptor,
        peerList,
        f, replicationModule, regionCreator);

    // TODO never ever call sleep
    Thread.sleep(1000);

    assertTrue(tablet.isOpen());
  }
}
