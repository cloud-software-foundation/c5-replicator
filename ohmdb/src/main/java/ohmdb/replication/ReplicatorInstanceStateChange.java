package ohmdb.replication;

import com.google.common.util.concurrent.Service;

/**
 * information about when a replicator instance changes state. replicator instances publish these to indicate
 * success or failure.
 *
 * A previously successful replicator can encounter a fatal error and then send one of these to notify other
 * components.
*/
public class ReplicatorInstanceStateChange {
    public final ReplicatorInstance instance;
    public final Service.State state;
    public final Throwable optError;

    /**
     *
     * @param instance the replicator instance that is affected
     * @param state the purported state we have entered
     * @param optError the optional error (can be null)
     */
    public ReplicatorInstanceStateChange(ReplicatorInstance instance, Service.State state, Throwable optError) {
        this.instance = instance;
        this.state = state;
        this.optError = optError;
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
