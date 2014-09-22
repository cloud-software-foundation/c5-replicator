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

import c5db.interfaces.replication.QuorumConfiguration;
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import io.netty.util.CharsetUtil;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static c5db.log.ReplicatorLogGenericTestUtil.aSeqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.anElectionTerm;
import static c5db.log.ReplicatorLogGenericTestUtil.lotsOfData;
import static c5db.log.ReplicatorLogGenericTestUtil.someData;

/**
 * Helper methods to create and manipulate OLogEntry instances.
 */
public class LogTestUtil {

  public static List<OLogEntry> emptyEntryList() {
    return new ArrayList<>();
  }

  public static OLogEntry makeEntry(long seqNum, long term, ByteBuffer data) {
    return new OLogEntry(seqNum, term, new OLogRawDataContent(Lists.newArrayList(data)));
  }

  public static OLogEntry makeEntry(long seqNum, long term, String stringData) {
    return makeEntry(seqNum, term, ByteBuffer.wrap(stringData.getBytes(CharsetUtil.UTF_8)));
  }

  public static List<OLogEntry> makeSingleEntryList(long seqNum, long term, String stringData) {
    return Lists.newArrayList(makeEntry(seqNum, term, stringData));
  }

  public static List<OLogEntry> makeSingleEntryList(long seqNum, long term, ByteBuffer data) {
    return Lists.newArrayList(makeEntry(seqNum, term, data));
  }

  /**
   * Create and return (end - start) entries, in ascending sequence number order, from start inclusive,
   * to end exclusive.
   */
  public static List<OLogEntry> someConsecutiveEntries(long start, long end) {
    List<OLogEntry> entries = new ArrayList<>();
    for (long i = start; i < end; i++) {
      entries.add(makeEntry(i, LongMath.divide(i + 1, 2, RoundingMode.CEILING), someData()));
    }
    return entries;
  }

  public static List<OLogEntry> nConsecutiveEntries(long howMany) {
    return someConsecutiveEntries(1, 1 + howMany);
  }

  public static OLogEntry anOLogEntry() {
    return makeEntry(aSeqNum(), anElectionTerm(), someData());
  }

  public static OLogEntry anOLogEntryWithLotsOfData() {
    return makeEntry(aSeqNum(), anElectionTerm(), lotsOfData());
  }

  public static OLogEntry anOLogConfigurationEntry() {
    final QuorumConfigurationMessage message = aQuorumConfigurationMessage();
    return new OLogEntry(aSeqNum(), anElectionTerm(), new OLogProtostuffContent<>(message));
  }

  private static QuorumConfigurationMessage aQuorumConfigurationMessage() {
    return QuorumConfiguration
        .of(Lists.newArrayList(1L, 2L, 3L))
        .getTransitionalConfiguration(Lists.newArrayList(4L, 5L, 6L))
        .toProtostuff();
  }
}
