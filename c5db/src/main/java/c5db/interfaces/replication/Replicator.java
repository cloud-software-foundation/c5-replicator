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
import org.jetlang.channels.Channel;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A replicator instance that is used to keep logs in sync across a quorum.
 */
public interface Replicator {
  String getQuorumId();

  /**
   * TODO change the type of datum to a protobuf that is useful.
   * <p/>
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
