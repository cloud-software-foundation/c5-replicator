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

package c5db.interfaces.replication;

import c5db.interfaces.log.SequentialEntry;

import java.nio.ByteBuffer;
import java.util.List;


/**
 * A Sequential Entry with the minimum set of information understandable to clients of
 * a GeneralizedReplicator: data and sequence number in the log. This is a value type
 * intended for returning to replicator clients via readers, iterators, and events.
 * <p>
 * Compare {@link c5db.replication.generated.LogEntry}, which is a message containing
 * entry information for internal distribution among replicators: also compare
 * e.g. OLogEntry, which is a SequentialEntry for internal log use, which contains
 * information that should be hidden from replicator clients.
 */
public class ReplicatorEntry extends SequentialEntry {
  protected final List<ByteBuffer> data;

  public ReplicatorEntry(long seqNum, List<ByteBuffer> data) {
    super(seqNum);
    this.data = data;
  }

  public List<ByteBuffer> getData() {
    return data;
  }

  @Override
  public String toString() {
    return "ReplicatorEntry{" +
        "seqNum=" + seqNum +
        ", data=" + data +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || (o.getClass() != this.getClass())) {
      return false;
    }
    ReplicatorEntry that = (ReplicatorEntry) o;
    return this.seqNum == that.getSeqNum()
        && this.data.equals(that.data);
  }

  @Override
  public int hashCode() {
    return ((int) seqNum) * 31 +
        data.hashCode();
  }
}
