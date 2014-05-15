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
package c5db.client;

import c5db.C5TestServerConstants;
import c5db.MiniClusterBase;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TestFlush extends MiniClusterBase {


  @Test
  public void testFlush() throws IOException, InterruptedException, TimeoutException, ExecutionException, MutationFailedException {

    Random random = new Random();
    byte[] randomBytes = new byte[1024 * 1024];
    random.nextBytes(randomBytes);
    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");

    ByteString tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));

    int port = getRegionServerPort();
    try (FakeHTable table = new FakeHTable(C5TestServerConstants.LOCALHOST, port, tableName)) {
      ArrayList<Put> puts = new ArrayList<>();
      for (int j = 1; j != 24; j++) {
        for (int i = 1; i != 12; i++) {
          puts.add(new Put(Bytes.vintToBytes(i * j)).add(cf, cq, randomBytes));
        }

        for (Put put : puts) {
          table.put(put);
        }

        puts.clear();
      }
    } finally {
      table.close();
    }

  }
}
