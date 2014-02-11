/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
package c5db.client;

import c5db.MiniClusterBase;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Populator extends MiniClusterBase {
  static ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));

  public Populator() throws IOException, InterruptedException {
  }

  public static void main(String[] args)
      throws IOException, InterruptedException {
    if (args.length < 1) {
      System.out.println("Include the port to talk to please");
      return;
    }
    int port = Integer.parseInt(args[0]);
    try (C5Table table = new C5Table(tableName, port)) {

      long start = System.currentTimeMillis();

      int numberOfBatches = 1;
      int batchSize = 1024 * 5;
      if (args.length == 2) {
        numberOfBatches = Integer.parseInt(args[0]);
        batchSize = Integer.parseInt(args[1]);

      }
      compareToHBasePut(table,
          Bytes.toBytes("cf"),
          Bytes.toBytes("cq"),
          Bytes.toBytes("value"),
          numberOfBatches,
          batchSize);
      long end = System.currentTimeMillis();
      System.out.println("time:" + (end - start));

    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
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
  public void testPopulator() throws IOException, InterruptedException {
    Populator populator = new Populator();
    populator.main(new String[]{String.valueOf(getRegionServerPort())});
  }
}
