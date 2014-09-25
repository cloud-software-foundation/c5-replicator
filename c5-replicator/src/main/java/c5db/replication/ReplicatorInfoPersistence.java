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

/**
 * Persist snippets of information for the replication algorithm.  These bits are critical to recover during
 * crash-recovery, so the interface is sync and expected to durably write the data to disk before returning.
 */
public interface ReplicatorInfoPersistence {
  public long readCurrentTerm(String quorumId) throws IOException;

  public long readVotedFor(String quorumId) throws IOException;

  public void writeCurrentTermAndVotedFor(String quorumId, long currentTerm, long votedFor) throws IOException;
}
