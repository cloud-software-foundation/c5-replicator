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

import com.dyuproject.protostuff.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class CompareToHBase {
  private static HTable hTable;
  private static ByteString tableName =
      ByteString.copyFrom(Bytes.toBytes("tableName"));
  private static Configuration conf;

  byte[] cf = Bytes.toBytes("cf");

  public CompareToHBase() throws IOException, InterruptedException {
    conf = HBaseConfiguration.create();
  }

  public static void main(String[] args) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    CompareToHBase testingUtil = new CompareToHBase();
    hTable = new HTable(conf, tableName.toByteArray());
    testingUtil.compareToHBaseScan();
    hTable.close();
  }

  public void compareToHBaseScan() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table = new C5Table(tableName);

    long As, Ae, Bs, Be;
    int i = 0;
    Result result;
    ResultScanner scanner;


    scanner = table.getScanner(cf);
    As = System.currentTimeMillis();
    do {
      i++;
      if (i % 1024 == 0) {
        System.out.print("#");
        System.out.flush();
      }
      if (i % (1024 * 80) == 0) {
        System.out.println("");
      }
      result = scanner.next();
    } while (result != null);
    scanner.close();
    Ae = System.currentTimeMillis();

    int j = 0;
    Bs = System.currentTimeMillis();
    scanner = hTable.getScanner(cf);
    do {
      j++;
      if (j % 1024 == 0) {
        System.out.print("!");
        System.out.flush();
      }
      if (j % (1024 * 80) == 0) {
        System.out.println("");
      }
      result = scanner.next();
    } while (result != null);
    Be = System.currentTimeMillis();

    scanner.close();

    System.out.println("A:" + String.valueOf(Ae - As) + "i:" + i);
    System.out.println("B:" + String.valueOf(Be - Bs) + "j:" + j);
    table.close();
  }
}