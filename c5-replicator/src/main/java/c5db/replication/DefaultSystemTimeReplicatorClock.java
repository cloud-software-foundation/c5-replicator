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

import c5db.ReplicatorConstants;

import java.util.Random;

class DefaultSystemTimeReplicatorClock implements ReplicatorClock {
  private final long electionTimeout;

  DefaultSystemTimeReplicatorClock() {
    Random r = new Random();
    int baseElectionTimeout = ReplicatorConstants.REPLICATOR_DEFAULT_BASE_ELECTION_TIMEOUT_MILLISECONDS;
    this.electionTimeout = r.nextInt(baseElectionTimeout) + baseElectionTimeout;
  }

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public long electionCheckInterval() {
    return ReplicatorConstants.REPLICATOR_DEFAULT_ELECTION_CHECK_INTERVAL_MILLISECONDS;
  }

  @Override
  public long electionTimeout() {
    return electionTimeout;
  }

  @Override
  public long leaderLogRequestsProcessingInterval() {
    return ReplicatorConstants.REPLICATOR_DEFAULT_LEADER_LOG_INTERVAL_MILLISECONDS;
  }
}
