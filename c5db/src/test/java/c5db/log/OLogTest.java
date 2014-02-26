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

import c5db.C5CommonTestUtil;
import c5db.generated.Log;
import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class OLogTest {
  private static Path testDirectory;
  private OLog log;

  @BeforeClass
  public static void setTestDirectory() {
    if (testDirectory == null) {
      testDirectory = (new C5CommonTestUtil()).getDataTestDir("olog");
    }
  }

  /**
   * Method to quickly create an OLogEntry, to make the test methods in this class more readable
   */
  private Log.OLogEntry makeEntry(String quorumId, long index, long term, String stringData) {
    return Log.OLogEntry.newBuilder()
        .setQuorumId(quorumId)
        .setIndex(index)
        .setTombStone(false)
        .setTerm(term)
        .setValue(ByteString.copyFrom(stringData.getBytes()))
        .build();
  }

  private byte[] getBytes(ByteBuffer byteBuffer) {
    byte[] b = new byte[byteBuffer.remaining()];
    byteBuffer.get(b);
    return b;
  }

  @Before
  public final void setUp() throws IOException {
    // interestingly clearOldLogs never actually does anything because it only
    // affects the 'archive' path. this call will move stuff from wal -> old_wal
    OLog.moveAwayOldLogs(testDirectory);
    log = new OLog(testDirectory);
  }

  @After
  public final void tearDown() throws IOException {
    OLog.moveAwayOldLogs(testDirectory);
  }

  @Test
  public void testGetLogEntryEmptyLog() {
    assertNull(log.getLogEntry(1, "quorumId"));
    assertNull(log.getLogEntry(0, "quorumId"));
  }

  @Test
  public void testGetLogEntry() {
    String quorumId = "quorum";
    String anotherQuorumId = "another quorum";

    List<Log.OLogEntry> entries = Lists.newArrayList(
        makeEntry(anotherQuorumId, 1, 1, "first"),
        makeEntry(quorumId, 2, 1, "second"),
        makeEntry(quorumId, 4, 2, "third"));
    log.logEntry(entries, quorumId); // Presently, the quorumId parameter here is ignored.

    assertNull(log.getLogEntry(1, quorumId));
    assertEquals(entries.get(0).getTerm(), log.getLogEntry(1, anotherQuorumId).getTerm());
    assertArrayEquals(entries.get(0).getValue().toByteArray(),
        getBytes(log.getLogEntry(1, anotherQuorumId).getData()));

    assertEquals(entries.get(1).getTerm(), log.getLogEntry(2, quorumId).getTerm());
    assertArrayEquals(entries.get(1).getValue().toByteArray(),
        getBytes(log.getLogEntry(2, quorumId).getData()));
    assertNull(log.getLogEntry(2, anotherQuorumId));

    assertNull(log.getLogEntry(3, quorumId));

    assertEquals(entries.get(2).getTerm(), log.getLogEntry(4, quorumId).getTerm());
    assertArrayEquals(entries.get(2).getValue().toByteArray(),
        getBytes(log.getLogEntry(4, quorumId).getData()));
    assertNull(log.getLogEntry(4, anotherQuorumId));
  }

  @Test
  public void testGetLogEntries() {
    // Within this test, assume the log has no holes and all entries are in a particular quorum
    final String quorumId = "yet another quorum";
    final List<Log.OLogEntry> entries = Lists.newArrayList(
        makeEntry(quorumId, 1, 1, "first"),
        makeEntry(quorumId, 2, 1, "second"),
        makeEntry(quorumId, 3, 2, "third"),
        makeEntry(quorumId, 4, 2, "fourth"));
    final List<LogEntry> emptyList = Lists.newArrayList();
    final List<LogEntry> outputEntries = Lists.transform(entries,
        (e) -> new LogEntry(e.getTerm(), e.getIndex(), ByteBuffer.wrap(e.getValue().toByteArray())));

    log.logEntry(entries, quorumId);

    // TODO test OLog method to retrieve multiple entries
  }
}
