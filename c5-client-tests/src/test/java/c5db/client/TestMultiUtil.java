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

import c5db.ManyClusterBase;
import c5db.MiniClusterBase;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static c5db.testing.BytesMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestMultiUtil extends ManyClusterBase {
  private final byte[] row1 = Bytes.toBytes("row1");
  private final byte[] row2 = Bytes.toBytes("row2");

  @Test(timeout = 10000)
  public void testMultiPut() throws IOException {
    DataHelper.putsRowInDB(table, new byte[][]{row1, row2}, value);
    assertThat(DataHelper.valueReadFromDB(table, row), is(not(equalTo(value))));
    assertThat(DataHelper.valueReadFromDB(table, row1), is(equalTo(value)));
    assertThat(DataHelper.valueReadFromDB(table, row2), is(equalTo(value)));
  }

  @Test(timeout = 10000)
  public void testScan() throws IOException {
    DataHelper.putsRowInDB(table, new byte[][]{row1, row2}, value);
    ResultScanner resultScanner = DataHelper.getScanner(table, new byte[]{});
    assertThat(DataHelper.nextResult(resultScanner), is(equalTo(value)));
    assertThat(DataHelper.nextResult(resultScanner), is(equalTo(value)));
  }

  @Test(timeout = 10000)
  public void testMultiDelete() throws IOException {
    DataHelper.putsRowInDB(table, new byte[][]{row1, row2}, value);
    List<Delete> deletes = new ArrayList<>();
    deletes.add(new Delete(row1));
    deletes.add(new Delete(row2));
    table.delete(deletes);
    assertThat(DataHelper.valueReadFromDB(table, row1), is(not(equalTo(value))));
    assertThat(DataHelper.valueReadFromDB(table, row2), is(not(equalTo(value))));
  }
}
