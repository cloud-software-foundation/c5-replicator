/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
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
    persistence.corrupt(bytePositionToCorrupt);

    log.subSequence(entry.getSeqNum(), entry.getSeqNum() + 1); // exception
  }
}
