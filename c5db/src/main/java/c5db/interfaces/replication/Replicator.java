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

import c5db.replication.QuorumConfiguration;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Channel;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * A replicator instance that is used to keep logs in sync across a quorum.
 */
public interface Replicator {
  String getQuorumId();

  QuorumConfiguration getQuorumConfiguration();

  /**
   * Change the members of the quorum to a new collection of peers (which may include peers in
   * the current quorum).
   *
   * @param newPeers The collection of peer IDs in the new quorum.
   * @return a future which will return the log entry index of the quorum configuration entry,
   * when it is known; or, this method will return null if this replicator is not the leader.
   * The actual completion of the quorum change will be signaled by the commitment of the
   * log entry at the returned index.
   */
  ListenableFuture<Long> changeQuorum(Collection<Long> newPeers) throws InterruptedException;

  /**
   * Log a datum
   *
   * @param data Some data to log.
   * @return a listenable for the index number OR null if we aren't the leader.
   */
  ListenableFuture<Long> logData(List<ByteBuffer> data) throws InterruptedException;

  long getId();

  boolean isLeader();

  void start();

  Channel<State> getStateChannel();

  // What state is this instance in?
  public enum State {
    INIT,
    FOLLOWER,
    CANDIDATE,
    LEADER,
  }

//  public ImmutableList<Long> getQuorum();
}
