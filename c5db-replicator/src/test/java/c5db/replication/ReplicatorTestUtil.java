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

package c5db.replication;

import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import io.netty.util.CharsetUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static c5db.log.ReplicatorLogGenericTestUtil.someData;

public class ReplicatorTestUtil {

  public static LogEntry makeProtostuffEntry(long index, long term, String stringData) {
    return makeProtostuffEntry(index, term, ByteBuffer.wrap(stringData.getBytes(CharsetUtil.UTF_8)));
  }

  public static LogEntry makeProtostuffEntry(long index, long term, ByteBuffer data) {
    return new LogEntry(term, index, Lists.newArrayList(data), null);
  }

  public static LogEntry makeConfigurationEntry(long index, long term, QuorumConfiguration configuration) {
    return new LogEntry(term, index, new ArrayList<>(), configuration.toProtostuff());
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

    public LogSequenceBuilder indexes(long... indexes) {
      for (long index : indexes) {
        logSequence.add(makeProtostuffEntry(index, term, someData()));
      }
      return this;
    }

    public LogSequenceBuilder configurationAndIndex(QuorumConfiguration configuration, long index) {
      logSequence.add(new LogEntry(term, index, new ArrayList<>(), configuration.toProtostuff()));
      return this;
    }

    // Alternate name for code clarity in certain places
    public LogSequenceBuilder seqNums(long... seqNums) {
      return indexes(seqNums);
    }

    // Alternate name for code clarity in certain places
    public LogSequenceBuilder configurationAndSeqNum(QuorumConfiguration configuration, long seqNum) {
      return configurationAndIndex(configuration, seqNum);
    }

    public List<LogEntry> build() {
      return logSequence;
    }
  }
}
