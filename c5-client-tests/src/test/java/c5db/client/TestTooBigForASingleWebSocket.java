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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.client.DataHelper.valueReadFromDatabase;
import static c5db.testing.BytesMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestTooBigForASingleWebSocket extends MiniClusterBase {
  private static final byte[] randomBytes = new byte[65535 * 4];
  private static final Random random = new Random();


  static {
    random.nextBytes(randomBytes);
  }

  byte[] row = Bytes.toBytes("cf");
  C5Table c5Table;


  @Before
  public void setUp() throws Exception {
    c5Table = new C5Table(ByteString.copyFromUtf8("test"), getRegionServerPort());
  }

  @Test
  public void shouldSuccesfullyAcceptSmallPut() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    putRowAndValueIntoDatabase(row, row);
  }

  @Test
  public void shouldSuccesfullyAcceptSmallPutAndReadSameValue() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    byte[] valuePutIntoDatabase = row;

    putRowAndValueIntoDatabase(row, valuePutIntoDatabase);

    assertThat(valueReadFromDatabase(row, c5Table), is(equalTo(valuePutIntoDatabase)));
  }


  @Test
  public void testSendBigOne() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    byte[] valuePutIntoDatabase = randomBytes;

    putRowAndValueIntoDatabase(row, valuePutIntoDatabase);
  }

  @Test
  public void shouldSuccesfullyAcceptLargePutAndReadSameValue() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    byte[] valuePutIntoDatabase = randomBytes;

    putRowAndValueIntoDatabase(row, valuePutIntoDatabase);

    assertThat(valueReadFromDatabase(row, c5Table), is(equalTo(valuePutIntoDatabase)));
  }

  private void putRowAndValueIntoDatabase(byte[] row,
                                          byte[] valuePutIntoDatabase) throws IOException {
    Put put = new Put(row);
    put.add(row, row, valuePutIntoDatabase);
    c5Table.put(put);
  }

}
