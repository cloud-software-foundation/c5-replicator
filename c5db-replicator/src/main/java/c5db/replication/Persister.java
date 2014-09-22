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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static c5db.ReplicatorConstants.REPLICATOR_PERSISTER_FILE_NAME;

/**
 * A small persister that writes files to a directory. Persisting this information is needed to
 * provide crash recovery for {@link c5db.replication.ReplicatorInstance}.
 */
class Persister implements ReplicatorInfoPersistence {
  private final QuorumFileReaderWriter quorumFileReaderWriter;

  Persister(QuorumFileReaderWriter quorumFileReaderWriter) {
    this.quorumFileReaderWriter = quorumFileReaderWriter;
  }

  @Override
  public long readCurrentTerm(String quorumId) throws IOException {
    return getLongOfFile(quorumId, 0);
  }

  @Override
  public long readVotedFor(String quorumId) throws IOException {
    return getLongOfFile(quorumId, 1);
  }

  private long getLongOfFile(String quorumId, int whichLine) throws IOException {
    List<String> data = quorumFileReaderWriter.readQuorumFile(quorumId, REPLICATOR_PERSISTER_FILE_NAME);
    if (data.size() != 2) {
      return 0; // corrupt file?
    }

    try {
      return Long.parseLong(data.get(whichLine));
    } catch (NumberFormatException e) {
      return 0; // corrupt file sucks?
    }
  }

  @Override
  public void writeCurrentTermAndVotedFor(String quorumId, long currentTerm, long votedFor) throws IOException {
    List<String> data = new ArrayList<>(2);
    data.add(Long.toString(currentTerm));
    data.add(Long.toString(votedFor));
    quorumFileReaderWriter.writeQuorumFile(quorumId, REPLICATOR_PERSISTER_FILE_NAME, data);
  }
}
