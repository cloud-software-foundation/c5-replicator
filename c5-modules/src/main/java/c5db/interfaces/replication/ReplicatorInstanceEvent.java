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
 * information about when a replicator instance changes state. replicator instances publish
 * these to indicate a variety of conditions.
 * <p>
 * <p>
 * A variety of events can be published:
 * <ul>
 * <li>Quorum started</li>
 * <li>Election timeout: I am doing an pre-election poll to determine if an election could succeed</li>
 * <li>Election started: I am starting an leader election</li>
 * <li>Leader elected: either me or someone else</li>
 * <li>Leader deposed: as a leader, I was deposed by someone else and have unbecome leader</li>
 * <li>Quorum configuration committed: a new configuration of peers has been committed</li>
 * <li>Quorum failure: with Throwable</li>
 * </ul>
 */
public class ReplicatorInstanceEvent {
  public static enum EventType {
    QUORUM_START,
    ELECTION_TIMEOUT,
    ELECTION_STARTED,
    LEADER_ELECTED,
    LEADER_DEPOSED,
    QUORUM_CONFIGURATION_COMMITTED,
    QUORUM_FAILURE,
  }

  public final Replicator instance;
  public final EventType eventType;
  public final long eventTime;
  public final long newLeader;
  public final long leaderElectedTerm;
  public final QuorumConfiguration configuration;
  public final Throwable error;

  public ReplicatorInstanceEvent(EventType eventType,
                                 Replicator instance,
                                 long newLeader,
                                 long leaderElectedTerm,
                                 long eventTime,
                                 QuorumConfiguration configuration,
                                 Throwable error) {
    this.newLeader = newLeader;
    this.leaderElectedTerm = leaderElectedTerm;
    this.instance = instance;
    this.eventType = eventType;
    this.eventTime = eventTime;
    this.configuration = configuration;
    this.error = error;
  }

  @Override
  public String toString() {
    return "ReplicatorInstanceEvent{" +
        "eventType=" + eventType +
        ", instance=" + instance +
        ", eventTime=" + eventTime +
        ", newLeader=" + newLeader +
        ", leaderElectedTerm=" + leaderElectedTerm +
        ", configuration=" + configuration +
        ", error=" + error +
        '}';
  }
}
