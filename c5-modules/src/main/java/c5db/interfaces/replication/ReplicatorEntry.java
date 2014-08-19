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
