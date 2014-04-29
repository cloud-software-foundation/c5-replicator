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
 * information about when a replicator instance changes state. replicator instances publish
 * these to indicate a variety of conditions.
 * <p/>
 * <p/>
 * A variety of events can be published:
 * <ul>
 * <li>Quorum started</li>
 * <li>Leader elected</li>
 * <li>election timeout, doing new election (became candidate)</li>
 * <li>quorum failure with Throwable</li>
 * <li>As a leader, I was deposed by someone else and have unbecome leader</li>
 * </ul>
 */
public class ReplicatorInstanceEvent {
  public static enum EventType {
    QUORUM_START,
    LEADER_ELECTED,
    ELECTION_TIMEOUT,
    QUORUM_FAILURE,
    LEADER_DEPOSED
  }

  public final Replicator instance;
  public final EventType eventType;
  public final long eventTime;
  public final long newLeader;
  public final Throwable error;

  public ReplicatorInstanceEvent(EventType eventType,
                                 Replicator instance,
                                 long newLeader,
                                 long eventTime,
                                 Throwable error) {
    this.newLeader = newLeader;
    this.instance = instance;
    this.eventType = eventType;
    this.eventTime = eventTime;
    this.error = error;
  }

  @Override
  public String toString() {
    return "ReplicatorInstanceEvent{" +
        "instance=" + instance +
        ", eventType=" + eventType +
        ", eventTime=" + eventTime +
        ", newLeader=" + newLeader +
        ", error=" + error +
        '}';
  }
}
