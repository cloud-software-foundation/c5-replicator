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

import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Channel;

import java.util.List;

/**
 * Replication is the method by which we get multiple copies of the write-ahead-log to multiple
 * machines. It forms the core of C5's ability to survive machine failures.
 * <p/>
 * The replication module manages multiple replicator instances, each one responsible for a
 * small chunk of the data-space, also known as a tablet (or region).
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

}
