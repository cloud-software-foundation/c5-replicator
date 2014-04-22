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


  Random random = new Random();
  private byte[] randomBytes = new byte[1024 * 1024];

  {
    random.nextBytes(randomBytes);
  }

  private static ByteString tableName;

  public TestFlush() throws IOException, InterruptedException {
  }


  public static void compareToHBasePut(final TableInterface table,
                                       final byte[] cf,
                                       final byte[] cq,
                                       final byte[] value,
                                       final int numberOfBatches,
                                       final int batchSize)
      throws IOException {
    ArrayList<Put> puts = new ArrayList<>();

    long startTime = System.nanoTime();
    for (int j = 1; j != numberOfBatches + 1; j++) {
      puts.clear();
      for (int i = 1; i != batchSize + 1; i++) {
        puts.add(new Put(Bytes.vintToBytes(i * j)).add(cf, cq, value));
      }

      int i = 0;
      for (Put put : puts) {
        i++;
        if (i % 1024 == 0) {
          long timeDiff = (System.nanoTime()) - startTime;
          System.out.print("#(" + timeDiff + ")");
          System.out.flush();
          startTime = System.nanoTime();
        }
        if (i % (1024 * 80) == 0) {
          System.out.println("");
        }
        table.put(put);
      }

      puts.clear();
    }
  }

  @Test
  public void testPopulator() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    TestFlush populator = new TestFlush();
    tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));

    int port = getRegionServerPort();
    C5Table table = new C5Table(tableName, port);

    long start = System.currentTimeMillis();

    int numberOfBatches = 1;
    int batchSize = 1024;

    compareToHBasePut(table,
        Bytes.toBytes("cf"),
        Bytes.toBytes("cq"),
        randomBytes,
        numberOfBatches,
        batchSize);
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
  }
}
