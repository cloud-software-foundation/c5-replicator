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

import c5db.interfaces.log.Reader;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.messages.generated.ModuleType;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The log module is responsible for running all the threads and IO for replicated
 * logs. It is responsible for maintaining persistence in the face of node or machine
 * failure.
 * <p>
 * One use case for replicating logs is to implement a fault-tolerant distributed
 * write-ahead log.
 */
@ModuleTypeBinding(ModuleType.Log)
public interface LogModule extends C5Module {
  // TODO: Replicator interaction is specified by protostuff message (LogEntry) but Reader by SequentialEntry;
  // TODO  should Mooring move to SequentialEntry specification?

  /**
   * Obtain a new quorum-specific ReplicatorLog -- that is, an interface to the
   * LogModule's services, specialized for one Replicator to use.
   *
   * @param quorumId Quorum ID -- an identifier of which continuity of distributed
   *                 log the ReplicatorLog should interface with. In other words,
   *                 a quorum is the unit of maintaining a single sequence of logged
   *                 entries. Multiple replicators may cooperate to replicate that
   *                 sequence, each having its own local log.
   * @return A future which will return a new, open ReplicatorLog. The caller does not
   * need to close it or release of resources; the resources, if any, will be released
   * when the LogModule stops. If an exception occurs while creating the log, the
   * future will return the exception.
   */
  ListenableFuture<ReplicatorLog> getReplicatorLog(String quorumId);

  /**
   * Obtain a Reader, to access entries that have been logged for a given quorum.
   *
   * @param quorumId Quorum ID
   * @return A new Reader instance.
   */
  <E extends SequentialEntry> Reader<E> getLogReader(String quorumId, SequentialEntryCodec<E> entryCodec);
}
