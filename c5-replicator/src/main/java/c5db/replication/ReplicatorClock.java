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

/**
 * Information used by a {@link c5db.replication.ReplicatorInstance} to configure itself; for instance,
 * tunable timing-related parameters.
 */
public interface ReplicatorClock {
  public long currentTimeMillis();

  /**
   * How often to check if the election needs to be rerun.
   * <p>
   * TODO revisit this to see if necessary or can be set to another derivative value.
   *
   * @return
   */
  public long electionCheckInterval();

  /**
   * The election timeout.
   *
   * @return
   */
  public long electionTimeout();

  /**
   * How frequently we should check the append queue, and send RPCs to the clients.
   *
   * @return
   */
  public long leaderLogRequestsProcessingInterval();
}
