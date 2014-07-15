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
