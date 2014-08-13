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

import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Subscriber;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * A replicator instance that is used to keep logs in sync across a quorum.
 */
public interface Replicator {
  String getQuorumId();

  /**
   * Return a future containing the Replicator's current quorum configuration; that is,
   * the set of peers it recognizes as being part of its quorum. This set can change
   * if directed to locally (for instance using the method changeQuorum) or if a remote
   * leader initiates a quorum change.
   */
  ListenableFuture<QuorumConfiguration> getQuorumConfiguration();

  /**
   * Change the members of the quorum to a new collection of peers (which may include peers in
   * the current quorum).
   *
   * @param newPeers The collection of peer IDs in the new quorum.
   * @return a future which will return the replicator receipt for logging the transitional
   * quorum configuration entry, when the entry's index known. The transitional quorum
   * configuration combines the current group of peers with the given collection of new peers.
   * When that transitional configuration is committed, the quorum configuration is guaranteed
   * to go through; prior to that commit, it is possible that a fault will cancel the quorum
   * change operation.
   * <p>
   * The actual completion of the quorum change will be signaled by a ReplicatorInstanceEvent
   * indicating the commitment of the quorum configuration consisting of the given peers,
   */
  ListenableFuture<ReplicatorReceipt> changeQuorum(Collection<Long> newPeers) throws InterruptedException;

  /**
   * Submit data to be replicated.
   * TODO we may want a variation of this method which does not block
   *
   * @param data Some data to log.
   * @return a listenable for a receipt for the log request, OR null if we aren't the leader.
   */
  ListenableFuture<ReplicatorReceipt> logData(List<ByteBuffer> data) throws InterruptedException;

  /**
   * @return The numerical ID for the server, or node, on which this Replicator resides.
   */
  long getId();

  boolean isLeader();

  void start();

  Subscriber<State> getStateChannel();

  // What state is this instance in?
  public enum State {
    INIT,
    FOLLOWER,
    CANDIDATE,
    LEADER,
  }

  public Subscriber<ReplicatorInstanceEvent> getEventChannel();

  /**
   * Get the Replicator's commit notice channel. By matching these issued IndexCommitNotices
   * against the ReplicatorReceipts returned when logging entries or changing quorums, users
   * of the Replicator can determine whether those submissions were successfully replicated
   * to the quorum.
   */
  public Subscriber<IndexCommitNotice> getCommitNoticeChannel();
}
