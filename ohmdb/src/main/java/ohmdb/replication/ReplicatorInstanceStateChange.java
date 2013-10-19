package ohmdb.replication;

import static com.google.common.util.concurrent.Service.State;

/**
 * information about when a replicator instance changes state. replicator instances publish these to indicate
 * success or failure.
 *
 * A previously successful replicator can encounter a fatal error and then send one of these to notify other
 * components.
 *
 * A replicator can have 2 states:
 * <ul>
 *     <li>{@link State.FAILED}</li>
 *     <li>{@link State.RUNNING}</li>
 * </ul>
*/
public class ReplicatorInstanceStateChange {
    public final ReplicatorInstance instance;
    public final State state;
    public final Throwable optError;

    /**
     *
     * @param instance the replicator instance that is affected
     * @param state the state we have entered (FAILED|RUNNING)
     * @param optError the optional error (can be null)
     */
    public ReplicatorInstanceStateChange(ReplicatorInstance instance, State state, Throwable optError) {
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
