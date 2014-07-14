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
