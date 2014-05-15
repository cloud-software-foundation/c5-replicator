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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static c5db.client.DataHelper.checkAndDeleteRowAndValueIntoDatabase;
import static c5db.client.DataHelper.checkAndPutRowAndValueIntoDatabase;
import static c5db.client.DataHelper.putRowAndValueIntoDatabase;
import static c5db.client.DataHelper.valueReadFromDB;
import static c5db.testing.BytesMatchers.equalTo;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestCheckAnd extends MiniClusterBase {

  @Test
  public void shouldFailCheckAndPutNullRow() throws IOException {
   assertFalse(checkAndPutRowAndValueIntoDatabase(table, row, value, notEqualToValue));
  }

  @Test
  public void shouldFailCheckAndPutWrongRow() throws IOException {
    putRowAndValueIntoDatabase(table, row, notEqualToValue);
    assertFalse(checkAndPutRowAndValueIntoDatabase(table, row, value, value));
    assertThat(valueReadFromDB(table, row), is(equalTo(notEqualToValue)));
  }

  @Test
  public void simpleCheckAndPutCanSucceed() throws IOException {
    putRowAndValueIntoDatabase(table, row, value);
    assertTrue(checkAndPutRowAndValueIntoDatabase(table, row, value, notEqualToValue));
    assertThat(valueReadFromDB(table, row), is(equalTo(notEqualToValue)));
  }

  @Test
  public void shouldFailCheckAndDeleteNullRow() throws IOException {
    assertFalse(checkAndDeleteRowAndValueIntoDatabase(table, row, value));
  }

  @Test
  public void shouldFailCheckAndDeleteWrongRow() throws IOException {
    putRowAndValueIntoDatabase(table, row, notEqualToValue);
    assertFalse(checkAndDeleteRowAndValueIntoDatabase(table, row, value));
    assertThat(valueReadFromDB(table, row), is(equalTo(notEqualToValue)));
  }

  @Test
  public void testSimpleCheckAndDeleteCanSucceed() throws IOException {
     putRowAndValueIntoDatabase(table, row, value);
    assertTrue(checkAndDeleteRowAndValueIntoDatabase(table, row, value));
    assertThat(valueReadFromDB(table, row), not(equalTo(value)));
  }

}