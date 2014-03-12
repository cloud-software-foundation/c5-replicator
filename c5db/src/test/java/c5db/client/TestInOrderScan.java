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
 */
package c5db.client;

import c5db.MiniClusterBase;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TestInOrderScan extends MiniClusterBase {
  private static ByteString tableName =
      ByteString.copyFrom(Bytes.toBytes("tableName"));

  byte[] cf = Bytes.toBytes("cf");

  public static void main(String[] args) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    TestInOrderScan testingUtil = new TestInOrderScan();
    testingUtil.compareToHBaseScan();
  }

  public void compareToHBaseScan() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table = new C5Table(tableName, getRegionServerPort());

    Result result = null;
    ResultScanner scanner;

    scanner = table.getScanner(cf);
    byte[] previousRow = {};
    int counter = 0;
    do {
      if (result != null) {
        previousRow = result.getRow();
      }
      result = scanner.next();

      if (Bytes.compareTo(result.getRow(), previousRow) < 1) {
        System.out.println(counter);
        System.exit(1);
      }

    } while (result != null);
    table.close();
  }

  public void testInOrderScan() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    compareToHBaseScan();
  }
}
