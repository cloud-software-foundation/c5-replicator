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
import com.dyuproject.protostuff.ByteString;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;

public class TestTooBigForASingleWebSocket extends MiniClusterBase {
  private static final byte[] randomBytes = new byte[65535 * 4];
  private static final Random random = new Random();

  static {
    random.nextBytes(randomBytes);
  }

  @Test
  public void testSendSmallOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    C5Table c5Table = new C5Table(ByteString.copyFromUtf8("test"), getRegionServerPort());
    byte[] row = Bytes.toBytes("cf");
    Put put = new Put(row);
    put.add(row, row, row);
    c5Table.put(put);
  }

  @Test
  public void testSendAndRecieveSmallOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    C5Table c5Table = new C5Table(ByteString.copyFromUtf8("test"), getRegionServerPort());
    byte[] row = Bytes.toBytes("cf");
    Put put = new Put(row);
    put.add(row, row, row);
    c5Table.put(put);
    Get get = new Get(row);
    get.addColumn(row, row);
    Result result = c5Table.get(get);
    byte[] value = result.getValue(row, row);
    assertArrayEquals(value, row);
  }

  @Test
  public void testSendBigOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    C5Table c5Table = new C5Table(ByteString.copyFromUtf8("test"), getRegionServerPort());
    byte[] row = Bytes.toBytes("cf");
    Put put = new Put(row);
    put.add(row, row, randomBytes);
    c5Table.put(put);
  }

  @Test
  public void testSendAndRecieveBigOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    C5Table c5Table = new C5Table(ByteString.copyFromUtf8("test"), getRegionServerPort());
    byte[] row = Bytes.toBytes("cf");
    Put put = new Put(row);
    put.add(row, row, randomBytes);
    c5Table.put(put);
    Get get = new Get(row);
    get.addColumn(row, row);
    Result result = c5Table.get(get);
    byte[] value = result.getValue(row, row);
    assertArrayEquals(value, randomBytes);
  }
}
