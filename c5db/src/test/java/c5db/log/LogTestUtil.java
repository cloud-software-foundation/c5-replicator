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

import c5db.replication.QuorumConfiguration;
import c5db.replication.generated.LogEntry;
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import io.netty.util.CharsetUtil;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Helper methods to create and manipulate OLogEntry instances.
 */
public class LogTestUtil {
  private static final Random deterministicDataSequence = new Random(112233);

  public static List<OLogEntry> emptyEntryList() {
    return new ArrayList<>();
  }

  public static OLogEntry makeEntry(long seqNum, long term, String stringData) {
    return makeEntry(seqNum, term, ByteBuffer.wrap(stringData.getBytes(CharsetUtil.UTF_8)));
  }

  public static OLogEntry makeEntry(long seqNum, long term, ByteBuffer data) {
    return new OLogEntry(seqNum, term, new OLogRawDataContent(Lists.newArrayList(data)));
  }

  public static List<OLogEntry> makeSingleEntryList(long seqNum, long term, String stringData) {
    return Lists.newArrayList(makeEntry(seqNum, term, stringData));
  }

  public static List<OLogEntry> makeSingleEntryList(long seqNum, long term, ByteBuffer data) {
    return Lists.newArrayList(makeEntry(seqNum, term, data));
  }

  public static LogEntry makeProtostuffEntry(long seqNum, long term, String stringData) {
    return makeEntry(seqNum, term, stringData).toProtostuff();
  }

  public static LogEntry makeProtostuffEntry(long seqNum, long term, ByteBuffer data) {
    return makeEntry(seqNum, term, data).toProtostuff();
  }

  public static LogEntry makeConfigurationEntry(long seqNum, long term, QuorumConfigurationMessage configuration) {
    return new LogEntry(term, seqNum, new ArrayList<>(), configuration);
  }

  public static long seqNum(long seqNum) {
    return seqNum;
  }

  public static long term(long term) {
    return term;
  }

  public static ByteBuffer someData() {
    byte[] bytes = new byte[10];
    deterministicDataSequence.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
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

  public static long anElectionTerm() {
    return Math.abs(deterministicDataSequence.nextLong());
  }

  public static long aSeqNum() {
    return Math.abs(deterministicDataSequence.nextLong());
  }

  public static OLogEntry anOLogEntry() {
    return makeEntry(aSeqNum(), anElectionTerm(), someData());
  }

  public static LogSequenceBuilder entries() {
    return new LogSequenceBuilder();
  }

  public static class LogSequenceBuilder {
    private final List<LogEntry> logSequence = new ArrayList<>();
    private long term = 1;

    public LogSequenceBuilder term(long term) {
      this.term = term;
      return this;
    }

    public LogSequenceBuilder indexes(long... indexList) {
      for (long index : indexList) {
        logSequence.add(makeProtostuffEntry(index, term, someData()));
      }
      return this;
    }

    public LogSequenceBuilder configurationAndIndex(QuorumConfiguration configuration, long index) {
      logSequence.add(new LogEntry(term, index, new ArrayList<>(), configuration.toProtostuff()));
      return this;
    }

    public List<LogEntry> build() {
      return logSequence;
    }
  }
}
