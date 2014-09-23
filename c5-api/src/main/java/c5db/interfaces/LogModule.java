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
