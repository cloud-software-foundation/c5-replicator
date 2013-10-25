package ohmdb;

import ohmdb.replication.IndexCommitNotice;
import ohmdb.replication.ReplicatorInstanceStateChange;

/**
 * Created with IntelliJ IDEA.
 * User: ryan
 * Date: 10/24/13
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ReplicationService extends OhmService {
    org.jetlang.channels.Channel<IndexCommitNotice> getIndexCommitNotices();

    /**
     * When a replicator changes state (eg: goes from
     * @return
     */
    org.jetlang.channels.Channel<ReplicatorInstanceStateChange> getReplicatorStateChanges();
}
