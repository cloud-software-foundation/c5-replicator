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

package c5db.log;

import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static c5db.log.LogTestUtil.makeProtostuffEntry;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MooringTest {
  ReplicatorLog log;

  private static OLog makeMockLog() {
    // Create a mock OLog, to be used by Mooring, whose logEntry method simulates a logging operation that does
    // not "complete" synchronously. It simulates this by returning an "unset" future.
    OLog mock = mock(OLog.class);
    when(mock.logEntry(anyListOf(OLogEntry.class), anyString()))
        .thenReturn(SettableFuture.create());
    return mock;
  }

  @Before
  public final void setUp() {
    log = new Mooring(makeMockLog(), "quorumId");
  }

  @After
  public final void tearDown() {
  }

  @Test
  public void testGetLastTermEmptyLog() {
    assert log.getLastIndex() == 0;
    assertEquals(0, log.getLastTerm());
  }

  @Test
  public void testGetLastTerm() throws Exception {
    // Verify that Mooring correctly gives the term of the most recent entry inserted into the log (which should
    // also be the term with the highest index, though that is not tested here).
    ByteBuffer data = ByteBuffer.wrap("123".getBytes());
    List<LogEntry> entries = Lists.newArrayList(
        makeProtostuffEntry(1, 1, data),
        makeProtostuffEntry(2, 2, data),
        makeProtostuffEntry(3, 2, data),
        makeProtostuffEntry(4, 2, data),
        makeProtostuffEntry(5, 3, data));
    log.logEntries(entries);
    assertEquals(3, log.getLastTerm());
  }

  @Test
  public void logEmptyEntryList() throws Exception {
    ByteBuffer data = ByteBuffer.wrap("123".getBytes());
    List<LogEntry> entries = Lists.newArrayList(
        makeProtostuffEntry(1, 1, data),
        makeProtostuffEntry(2, 2, data),
        makeProtostuffEntry(3, 2, data));
    log.logEntries(entries);
    assertEquals(2, log.getLastTerm());
    assertEquals(3, log.getLastIndex());

    // Make sure it's okay to pass an empty list
    log.logEntries(Lists.newArrayList());
    assertEquals(2, log.getLastTerm());
    assertEquals(3, log.getLastIndex());
  }
}
