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

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HBasePop {
  static ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  static Configuration conf = HBaseConfiguration.create();
  static byte[] cf = Bytes.toBytes("cf");

  public static void main(String[] args) throws IOException, InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName.toByteArray());
    hTableDescriptor.addFamily(new HColumnDescriptor(cf));
    admin.createTable(hTableDescriptor);

    long start = System.currentTimeMillis();
    compareToHBasePut();
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
  }

  public static void compareToHBasePut() throws IOException {
    byte[] cq = Bytes.toBytes("cq");
    byte[] value = Bytes.toBytes("value");

    try (HTable table = new HTable(conf, tableName.toByteArray())) {
      ArrayList<Put> puts = new ArrayList<>();
      for (int j = 1; j != 30 + 1; j++) {
        for (int i = 1; i != (1024 * 81) + 1; i++) {
          puts.add(new Put(Bytes.vintToBytes(i * j)).add(cf, cq, value));
        }

        int i = 0;
        for (Put put : puts) {
          i++;
          if (i % 1024 == 0) {
            System.out.print("#");
            System.out.flush();
          }
          if (i % (1024 * 80) == 0) {
            System.out.println("");
          }
          table.put(put);
        }
        puts.clear();
      }
    }
  }
}
