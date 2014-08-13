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

import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorReceipt;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.fibers.Fiber;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public class SingleNodeFakeReplicator implements Replicator {
  private final long nodeId;
  private final String quorumId;

  private final Channel<State> stateChannel = new MemoryChannel<>();
  private final Channel<ReplicatorInstanceEvent> eventChannel = new MemoryChannel<>();
  private final Channel<IndexCommitNotice> commitNoticeChannel = new MemoryChannel<>();
  private final Fiber fiber;

  private long nextSeqNum = 1;
  private long term = 1;
  private State state = State.FOLLOWER;

  public SingleNodeFakeReplicator(Fiber fiber, long nodeId, String quorumId) {
    this.fiber = fiber;
    this.nodeId = nodeId;
    this.quorumId = quorumId;
  }

  @Override
  public String getQuorumId() {
    return quorumId;
  }

  @Override
  public ListenableFuture<QuorumConfiguration> getQuorumConfiguration() {
    return Futures.immediateFuture(
        QuorumConfiguration.of(
            Sets.newHashSet(nodeId)));
  }

  @Override
  public ListenableFuture<ReplicatorReceipt> changeQuorum(Collection<Long> newPeers) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized ListenableFuture<ReplicatorReceipt> logData(List<ByteBuffer> data) {
    SettableFuture<ReplicatorReceipt> receiptFuture = SettableFuture.create();
    final long thisSeqNum = nextSeqNum;
    nextSeqNum++;

    doLater(() -> {
      receiptFuture.set(new ReplicatorReceipt(term, thisSeqNum));
      doLater(() ->
          commitNoticeChannel.publish(new IndexCommitNotice(quorumId, nodeId, thisSeqNum, thisSeqNum, term)));
    });

    return receiptFuture;
  }

  @Override
  public long getId() {
    return nodeId;
  }

  public void start() {
    state = State.LEADER;
    doLater(() ->
        stateChannel.publish(state));
  }

  @Override
  public Subscriber<State> getStateChannel() {
    return stateChannel;
  }

  @Override
  public Subscriber<ReplicatorInstanceEvent> getEventChannel() {
    return eventChannel;
  }

  @Override
  public Subscriber<IndexCommitNotice> getCommitNoticeChannel() {
    return commitNoticeChannel;
  }

  private void doLater(Runnable runnable) {
    fiber.execute(runnable);
  }
}
