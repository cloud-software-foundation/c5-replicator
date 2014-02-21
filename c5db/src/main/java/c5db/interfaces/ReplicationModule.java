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
package c5db.interfaces;

import c5db.messages.generated.ModuleType;
import c5db.replication.ReplicatorInstance;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Channel;

import java.util.List;

/**
 * The replication module/module.  The API to other modules internal to c5db.
 */
@DependsOn({DiscoveryModule.class, LogModule.class})
@ModuleTypeBinding(ModuleType.Replication)
public interface ReplicationModule extends C5Module {
    ListenableFuture<Replicator> createReplicator(String quorumId,
                                                  List<Long> peers);

    public Channel<IndexCommitNotice> getIndexCommitNotices();

    /**
     * When a replicator changes state (eg: goes from
     *
     * @return
     */
    public Channel<ReplicatorInstanceEvent> getReplicatorEventChannel();

    /**
     * information about when a replicator instance changes state. replicator instances publish
     * these to indicate a variety of conditions.
     *
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
    public static class ReplicatorInstanceEvent {
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

    /**
     * A broadcast that indicates that a particular index has become visible.
     */
    public static class IndexCommitNotice {
        public final ReplicatorInstance replicatorInstance;
        public final long committedIndex;

        public IndexCommitNotice(ReplicatorInstance replicatorInstance, long committedIndex) {
            this.replicatorInstance = replicatorInstance;
            this.committedIndex = committedIndex;
        }

        @Override
        public String toString() {
            return "IndexCommitNotice{" +
                    "replicatorInstance=" + replicatorInstance +
                    ", committedIndex=" + committedIndex +
                    '}';
        }
    }

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
         * @param datum some data to log.
         * @return a listenable for the index number OR null if we aren't the leader.
         */
        ListenableFuture<Long> logData(byte[] datum) throws InterruptedException;

        long getId();

        boolean isLeader();

        void start();

        // TODO add these maybe in the future
        // public ImmutableList<Long> getQuorum();
    }
}
