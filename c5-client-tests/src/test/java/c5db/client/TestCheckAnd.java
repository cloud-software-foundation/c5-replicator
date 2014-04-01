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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.testing.BytesMatchers.equalTo;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestCheckAnd extends MiniClusterBase {
  private final byte[] cf = Bytes.toBytes("cf");
  private final byte[] cq = Bytes.toBytes("cq");
  private final byte[] value = Bytes.toBytes("value");
  private final byte[] notEqualToValue = Bytes.toBytes("notEqualToValue");
  @Rule
  public TestName name = new TestName();
  private C5Table table;
  private byte[] row;

  @Before
  public void before() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    final ByteString tableName = ByteString.copyFrom(Bytes.toBytes(name.getMethodName()));
    table = new C5Table(tableName, getRegionServerPort());
    row = Bytes.toBytes(name.getMethodName());
  }

  @After
  public void after() {
    table.close();
  }

  @Test
  public void shouldFailCheckAndPutNullRow()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    assertFalse(checkAndPutRowAndValueIntoDatabase(table, row, value, notEqualToValue));
  }

  @Test
  public void shouldFailCheckAndPutWrongRow()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    putRowAndValueIntoDatabase(table, row, notEqualToValue);
    assertFalse(checkAndPutRowAndValueIntoDatabase(table, row, value, value));
    assertThat(valueReadFromDatabase(row, table), is(equalTo(notEqualToValue)));
  }

  @Test
  public void simpleCheckAndPutCanSucceed()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    putRowAndValueIntoDatabase(table, row, value);
    assertTrue(checkAndPutRowAndValueIntoDatabase(table, row, value, notEqualToValue));
    assertThat(valueReadFromDatabase(row, table), is(equalTo(notEqualToValue)));
  }

  @Test
  public void shouldFailCheckAndDeleteNullRow()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    assertFalse(checkAndDeleteRowAndValueIntoDatabase(table, row, value));
  }

  @Test
  public void shouldFailCheckAndDeleteWrongRow()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    putRowAndValueIntoDatabase(table, row, notEqualToValue);
    assertFalse(checkAndDeleteRowAndValueIntoDatabase(table, row, value));
    assertThat(valueReadFromDatabase(row, table), is(equalTo(notEqualToValue)));
  }

  @Test
  public void testSimpleCheckAndDeleteCanSucceed()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    putRowAndValueIntoDatabase(table, row, value);
    assertTrue(checkAndDeleteRowAndValueIntoDatabase(table, row, value));
    assertThat(valueReadFromDatabase(row, table), not(equalTo(value)));
  }

  private void putRowAndValueIntoDatabase(C5Table c5Table,
                                          byte[] row,
                                          byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    c5Table.put(put);
  }

  private boolean checkAndPutRowAndValueIntoDatabase(C5Table c5Table,
                                                     byte[] row,
                                                     byte[] valueToCheck,
                                                     byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(cf, cq, valuePutIntoDatabase);
    return c5Table.checkAndPut(row, cf, cq, valueToCheck, put);
  }

  private boolean checkAndDeleteRowAndValueIntoDatabase(C5Table c5Table,
                                                        byte[] row,
                                                        byte[] valueToCheck) throws IOException {
    Delete delete = new Delete(row);
    return c5Table.checkAndDelete(row, cf, cq, valueToCheck, delete);
  }

  byte[] valueReadFromDatabase(byte[] row, C5Table c5Table) throws IOException {
    Get get = new Get(row);
    get.addColumn(cf, cq);
    Result result = c5Table.get(get);
    return result.getValue(cf, cq);
  }
}
