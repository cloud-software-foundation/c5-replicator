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

import c5db.ConfigDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A small persister that writes files to a directory. Persisting this information is needed to
 * provide crash recovery for {@link c5db.replication.ReplicatorInstance}.
 */
class Persister implements ReplicatorInfoPersistence {

  private static final String REPLICATOR_PERSISTER_FILE_NAME = "replication-data";
  private final ConfigDirectory configDirectory;

  Persister(ConfigDirectory configDirectory) {
    this.configDirectory = configDirectory;
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
    List<String> data = configDirectory.readFile(configDirectory.getQuorumRelPath(quorumId),
        REPLICATOR_PERSISTER_FILE_NAME);
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
    configDirectory.writeFile(configDirectory.getQuorumRelPath(quorumId), REPLICATOR_PERSISTER_FILE_NAME, data);
  }
}
