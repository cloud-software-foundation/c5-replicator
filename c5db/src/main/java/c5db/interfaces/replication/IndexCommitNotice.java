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

import c5db.replication.ReplicatorInstance;

/**
 * A broadcast that indicates that a particular index has become visible.
 */
public class IndexCommitNotice {
  public final ReplicatorInstance replicatorInstance;
  public final long committedIndex;

  public IndexCommitNotice(ReplicatorInstance replicatorInstance, long committedIndex) {
    this.replicatorInstance = replicatorInstance;
    this.committedIndex = committedIndex;
  }

  @Override
  public String toString() {
    return "IndexCommitNotice{" +
        "replicatorInstance=" + replicatorInstance +
        ", committedIndex=" + committedIndex +
        '}';
  }
}
