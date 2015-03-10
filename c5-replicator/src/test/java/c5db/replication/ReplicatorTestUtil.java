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

package c5db.replication;

import c5db.interfaces.replication.QuorumConfiguration;
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
    return new LogEntry(term, index, new ArrayList<ByteBuffer>(), configuration.toProtostuff());
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
      logSequence.add(new LogEntry(term, index, new ArrayList<ByteBuffer>(), configuration.toProtostuff()));
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
