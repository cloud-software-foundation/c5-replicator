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

import c5db.interfaces.log.SequentialEntryCodec;
import com.google.common.collect.Lists;
import org.junit.Test;

import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.LogTestUtil.anOLogEntryWithLotsOfData;

public class LogPersistenceErrorTest {
  private final ByteArrayPersistence persistence = new ByteArrayPersistence();
  private final SequentialEntryCodec<OLogEntry> codec = new OLogEntry.Codec();
  private final SequentialLog<OLogEntry> log = new EncodedSequentialLog<>(
      persistence,
      codec,
      new InMemoryPersistenceNavigator<>(persistence, codec));

  @Test(expected = CrcError.class)
  public void logThrowsAnExceptionIfDetectingCorruptedContentWhenReading() throws Exception {
    OLogEntry entry = anOLogEntryWithLotsOfData();
    log.append(Lists.newArrayList(entry));

    int numberOfBytesFromEndOfPersistence = 5;
    int bytePositionToCorrupt = (int) persistence.size() - numberOfBytesFromEndOfPersistence;
    persistence.overwrite(bytePositionToCorrupt, 0);

    log.subSequence(entry.getSeqNum(), entry.getSeqNum() + 1); // exception
  }
}
