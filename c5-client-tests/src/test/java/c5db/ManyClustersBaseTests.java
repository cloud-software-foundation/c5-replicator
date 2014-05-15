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
package c5db;

import c5db.client.C5AsyncDatabase;
import c5db.client.FakeHTable;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ManyClustersBaseTests extends ManyClusterBase {

  @Test
  public void metaTableShouldContainUserTableEntries() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ByteString tableName = ByteString.copyFrom(Bytes.toBytes("hbase:meta"));
    FakeHTable c5AsyncDatabase = new FakeHTable(C5TestServerConstants.LOCALHOST, metaOnPort, tableName);
    ResultScanner scanner = c5AsyncDatabase.getScanner(HConstants.CATALOG_FAMILY);

    Result result;
    int counter = 0;
    do {
      result = scanner.next();
      if (result != null) {
        counter++;
      }
    } while (result != null);

    assertThat(counter, is(1));
  }
}
