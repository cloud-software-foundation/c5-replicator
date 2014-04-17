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

import com.google.common.collect.Lists;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class OLogEntryDescriptionTest {
  private final ByteArrayPersistence persistence = new ByteArrayPersistence();
  private final SequentialEntryCodec<OLogEntry> codec = new OLogEntry.Codec();
  private final SequentialEntryCodec<OLogEntryDescription> descriptionCodec = new OLogEntryDescription.Codec();
  private final SequentialLog<OLogEntry> log = new EncodedSequentialLog<>(
      persistence,
      codec,
      new InMemoryPersistenceNavigator<>(persistence, codec));
  private final SequentialLog<OLogEntryDescription> descriptionLog = new EncodedSequentialLog<>(
      persistence,
      descriptionCodec,
      new InMemoryPersistenceNavigator<>(persistence, descriptionCodec));

  private static final ByteBuffer DATA = ByteBuffer.wrap("data".getBytes(CharsetUtil.UTF_8));

  @Test
  public void returnsADescriptionOfALoggedEntry() throws Exception {
    log.append(makeSingleEntryList(seqNum(11), term(22), DATA));

    assertThat(descriptionLog.getLastEntry(), is(equalTo(
        new OLogEntryDescription(
            seqNum(11),
            term(22),
            DATA.remaining(),
            true,
            true)
    )));
  }

  @Test
  public void detectsThatLoggedContentHasBeenCorruptedWhenDescribingIt() throws Exception {
    log.append(makeSingleEntryList(seqNum(11), term(22), DATA));

    // Hack to find the content within the logged data: assume 4-byte ending CRC, so subtract 5
    // Also assume that the existing byte value at that location is different than zero.
    int bytePositionToCorrupt = (int) persistence.size() - 5;
    persistence.overwrite(bytePositionToCorrupt, 0);

    assertThat(descriptionLog.getLastEntry(), is(equalTo(
        new OLogEntryDescription(
            seqNum(11),
            term(22),
            DATA.remaining(),
            true,
            false)
    )));
  }

  @Test
  public void worksWithForEachMethodToDescribeTheEntireContentsOfTheLog() throws Exception {
    List<OLogEntry> entries = someConsecutiveEntries(10, 20);
    log.append(entries);

    List<OLogEntryDescription> descriptions = new ArrayList<>();
    descriptionLog.forEach(descriptions::add);

    assertThat(descriptions, is(equalTo(descriptionsCorrespondingTo(entries))));
  }

  private List<OLogEntryDescription> descriptionsCorrespondingTo(List<OLogEntry> entries) {
    return Lists.transform(entries, (entry) ->
        new OLogEntryDescription(entry.getSeqNum(), entry.getElectionTerm(), entry.contentLength(), true, true));
  }
}
