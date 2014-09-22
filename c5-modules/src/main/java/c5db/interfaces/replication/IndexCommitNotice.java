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
