package ohmdb.interfaces;

import com.google.common.util.concurrent.ListenableFuture;
import ohmdb.replication.ReplicatorInstance;
import org.jetlang.channels.Channel;

/**
 *
 */
public interface ReplicationService extends OhmService {
    Channel<IndexCommitNotice> getIndexCommitNotices();

    /**
     * When a replicator changes state (eg: goes from
     * @return
     */
    Channel<ReplicatorInstanceStateChange> getReplicatorStateChanges();

    /**
     * information about when a replicator instance changes state. replicator instances publish these to indicate
     * success or failure.
     *
     * A previously successful replicator can encounter a fatal error and then send one of these to notify other
     * components.
     *
     * A replicator can have 2 states:
     * <ul>
     *     <li>{@link com.google.common.util.concurrent.Service.State.FAILED}</li>
     *     <li>{@link com.google.common.util.concurrent.Service.State.RUNNING}</li>
     * </ul>
    */
    public static class ReplicatorInstanceStateChange {
        public final Replicator instance;
        public final State state;
        public final Throwable optError;

        /**
         *
         * @param instance the replicator instance that is affected
         * @param state the state we have entered (FAILED|RUNNING)
         * @param optError the optional error (can be null)
         */
        public ReplicatorInstanceStateChange(Replicator instance, State state, Throwable optError) {
            this.instance = instance;
            this.state = state;
            this.optError = optError;

            assert state == State.FAILED || state == State.RUNNING;
        }

        @Override
        public String toString() {
            return "ReplicatorInstanceStateChange{" +
                    "instance=" + instance +
                    ", state=" + state +
                    ", optError=" + optError +
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
         *
         * Log a datum
         * @param datum some data to log.
         * @return a listenable for the index number OR null if we aren't the leader.
         */
        ListenableFuture<Long> logData(byte[] datum) throws InterruptedException;

        long getId();

        boolean isLeader();

        // TODO add these maybe in the future
        // public ImmutableList<Long> getQuorum();
    }
}
