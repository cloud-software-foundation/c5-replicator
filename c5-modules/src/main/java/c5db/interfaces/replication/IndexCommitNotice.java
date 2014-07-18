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
 * A broadcast that indicates that a particular range of replicator indexes have become
 * visible, i.e., been committed. All of the indexes in this range must correspond to
 * a single term, so if a given range subsumes multiple terms and they are all committed
 * at once, multiple IndexCommitNotices will be needed -- one for each term.
 */
public class IndexCommitNotice {
  public final String quorumId;
  public final long nodeId;
  public final long firstIndex;
  public final long lastIndex;
  public final long term;

  public IndexCommitNotice(String quorumId,
                           long nodeId,
                           long firstIndex,
                           long lastIndex,
                           long term) {
    this.quorumId = quorumId;
    this.nodeId = nodeId;
    this.firstIndex = firstIndex;
    this.lastIndex = lastIndex;
    this.term = term;
  }

  @Override
  public String toString() {
    return "IndexCommitNotice{" +
        "quorumId=" + quorumId +
        ", nodeId=" + nodeId +
        ", firstIndex=" + firstIndex +
        ", lastIndex=" + lastIndex +
        ", term=" + term +
        '}';
  }
}
