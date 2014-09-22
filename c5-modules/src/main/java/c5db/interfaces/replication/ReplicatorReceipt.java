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


/**
 * Value-type immutable object returned when logging data to a Replicator, It has enough
 * information for the client of the replicator to determine if and when the data has been
 * durably replicated (that is, in the language of the consensus algorithm, committed).
 */
public class ReplicatorReceipt {
  public final long term;
  public final long seqNum;

  public ReplicatorReceipt(long term, long seqNum) {
    this.term = term;
    this.seqNum = seqNum;
  }

  @Override
  public String toString() {
    return "ReplicatorReceipt{" +
        "term=" + term +
        ", seqNum=" + seqNum +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReplicatorReceipt that = (ReplicatorReceipt) o;
    return term == that.term
        && seqNum == that.seqNum;
  }

  @Override
  public int hashCode() {
    int result = (int) (term ^ (term >>> 32));
    result = 31 * result + (int) (seqNum ^ (seqNum >>> 32));
    return result;
  }
}
