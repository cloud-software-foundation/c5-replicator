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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.client.DataHelper.putRowInDB;
import static c5db.client.DataHelper.valueExistsInDB;
import static c5db.client.DataHelper.valueReadFromDB;
import static c5db.client.DataHelper.valuesExistsInDB;
import static c5db.client.DataHelper.valuesReadFromDB;
import static c5db.testing.BytesMatchers.equalTo;
import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;

public class TestingUtil extends MiniClusterBase {

  @Test
  public void testSimplePutGet() throws IOException {
    putRowInDB(table, row);
    assertThat(valueReadFromDB(table, row), is(equalTo(value)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPut() throws IOException {
    putRowInDB(table, new byte[]{});
  }

  // TODO Should be an IllegalArgument HBase!!!
  @Test(expected = NullPointerException.class)
  public void testNullPut() throws IOException {
    putRowInDB(table, null);
  }

  @Test
  public void testExist() throws IOException {
    final byte[] randomBytesNeverInsertedInDB = {0x00, 0x01, 0x02};
    putRowInDB(table, row);
    assertTrue(valueExistsInDB(table, row));
    assertFalse(valueExistsInDB(table, randomBytesNeverInsertedInDB));
  }


  @Test(expected = IllegalArgumentException.class)
  public void testInvalidExist() throws IOException {
    putRowInDB(table, row);
    assertTrue(valueExistsInDB(table, row));
    valueExistsInDB(table, new byte[]{});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullExist() throws IOException {
    putRowInDB(table, row);
    assertTrue(valueExistsInDB(table, row));
    valueExistsInDB(table, null);
  }

  @Test
  public void testScan() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    byte[] row1 = new byte[]{0x00};
    byte[] row2 = new byte[]{0x01};
    byte[] row3 = new byte[]{0x02};
    byte[] stopRow = new byte[]{0x03};

    putRowInDB(table, row1);
    putRowInDB(table, row2);
    putRowInDB(table, row3);

    Scan scan = new Scan(row1);
    scan.setStopRow(stopRow);
    ResultScanner resultScanner = table.getScanner(scan);

    assertArrayEquals(resultScanner.next().getRow(), row1);
    assertArrayEquals(resultScanner.next().getRow(), row2);
    assertArrayEquals(resultScanner.next().getRow(), row3);
  }

  @Test
  public void testMultiGet() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    byte[] neverInserted = Bytes.toBytes("rowNeverInserted");
    putRowInDB(table, row);
    byte[][] values = valuesReadFromDB(table, new byte[][]{row, neverInserted});
    assertArrayEquals(value, values[0]);
    assertNull(values[1]);
  }

  @Test
  public void testMultiExists() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    byte[] neverInserted = Bytes.toBytes("rowNeverInserted");
    putRowInDB(table, row);
    Boolean[] values = valuesExistsInDB(table, new byte[][]{row, neverInserted});
    assertTrue(values[0]);
    assertFalse(values[1]);
  }
}
