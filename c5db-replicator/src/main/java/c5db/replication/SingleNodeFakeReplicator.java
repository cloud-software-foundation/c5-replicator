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
