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
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class TestingUtil extends MiniClusterBase {
  ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));

  byte[] cf = Bytes.toBytes("cf");
  byte[] cq = Bytes.toBytes("cq");
  byte[] value = Bytes.toBytes("value");

  @Test
  public void testSimplePutGet() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());
    byte[] row = Bytes.toBytes("testSimplePutGet");
    table.put(new Put(row).add(cf, cq, value));
    Result result = table.get(new Get(row).addColumn(cf, cq));
    assertArrayEquals(result.getRow(), row);
    table.close();
  }


  @Test
  public void testSimplePutGet2() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());
    byte[] row = Bytes.toBytes("testSimplePutGet2");
    table.put(new Put(row).add(cf, cq, value));
    Result result = table.get(new Get(row).addColumn(cf, cq));
    assertArrayEquals(result.getRow(), row);
    table.close();
  }


  @Test
  public void testSimplePutGet3() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());
    byte[] row = Bytes.toBytes("testSimplePutGet3");
    table.put(new Put(row).add(cf, cq, value));
    Result result = table.get(new Get(row).addColumn(cf, cq));
    assertArrayEquals(result.getRow(), row);
    table.close();
  }

  @Test
  public void testExist() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());
    byte[] row = Bytes.toBytes("testExist");
    table.put(new Put(row).add(cf, cq, value));
    boolean result = table.exists(new Get(row).addColumn(cf, cq));
    assertTrue(result);

    result = table.exists(new Get(Bytes.add(row, row)).addColumn(cf, cq));
    assertFalse(result);
    table.close();
  }

  @Test
  public void testScan() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());

    byte[] row = Bytes.toBytes("testScan1");

    table.put(new Put(row).add(cf, cq, value));
    table.put(new Put(Bytes.add(row, new byte[]{0x00})).add(cf, cq, value));
    table.put(new Put(Bytes.add(row, new byte[]{0x00, 0x01})).add(cf, cq, value));
    Scan scan = new Scan(row);
    scan.setStopRow(Bytes.add(row, new byte[] { 0x02}));

    scan.addColumn(cf, cq);
    ResultScanner resultScanner = table.getScanner(scan);
    Result r = resultScanner.next();
    assertArrayEquals(r.getRow(), row);
    r = resultScanner.next();
    assertArrayEquals(r.getRow(), Bytes.add(row, new byte[]{0x00}));
    r = resultScanner.next();
    assertArrayEquals(r.getRow(), Bytes.add(row, new byte[]{0x00, 0x01}));
    table.close();
  }

  @Test
  public void testMultiGet() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());

    byte[] row = Bytes.toBytes("testMultiGet");
    table.put(new Put(row).add(cf, cq, value));

    List<Get> gets = new ArrayList<>();
    gets.add(new Get(row).addColumn(cf, cq));
    gets.add(new Get(Bytes.add(row, row)).addColumn(cf, cq));
    Result[] result = table.get(gets);

    result.toString();
    table.close();
  }

  @Test
  public void testMultiExists() throws IOException, InterruptedException, TimeoutException, ExecutionException {

    C5Table table;
    table = new C5Table(tableName, getRegionServerPort());
    byte[] row = Bytes.toBytes("testMultiExists");

    List<Get> gets = new ArrayList<>();
    gets.add(new Get(row).addColumn(cf, cq));
    gets.add(new Get(Bytes.add(Bytes.add(row, row), row)).addColumn(cf, cq));
    gets.add(new Get(Bytes.add(row, row)).addColumn(cf, cq));
    Boolean[] result = table.exists(gets);

    result.toString();
    table.close();
  }

}
