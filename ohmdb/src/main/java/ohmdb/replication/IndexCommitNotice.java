package ohmdb.replication;

/**
 * A broadcast that indicates that a particular index has become visible.
 */
public class IndexCommitNotice {
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
