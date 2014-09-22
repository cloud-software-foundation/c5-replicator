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

/**
 * The c5db.log package contains services that provide logging for c5db.replication.
 * <p>
 * C5 replication is an implementation of the Raft consensus protocol, with some
 * modifications. It works by bringing multiple nodes into perfect agreement on a
 * sequence of entries, each of which can represent e.g. edits to a database,
 * or some other state machine transition. That sequence of entries is called the
 * nodes' log.
 * <p>
 * There are two main entry points for external agents to interface with the log.
 * One is {@link c5db.interfaces.LogModule}, which describes a service for starting
 * and stopping the log, connecting it up, and accessing its data. The second is
 * {@link c5db.interfaces.replication.ReplicatorLog} which is the interface to
 * the log that an individual Replicator sees and uses; in other words, ReplicatorLog
 * describes the logging capabilities needed by the Raft algorithm.
 * <p>
 * At present, the c5db.log package provides one implementation of the Raft log,
 * accessed through interface {@link c5db.log.OLog}; external agents essentially
 * interact with OLog through adapters. The provided implementation is
 * {@link c5db.log.QuorumDelegatingLog}.
 * <p>
 * An important concept is that of "quorums." A quorum is essentially what the Raft
 * algorithm describes as a "cluster." It is the set of independent entities (what
 * C5 represents by {@link c5db.interfaces.replication.Replicator}) that coordinate
 * to replicate entries. A quorum is identified by a string unique to it called its
 * quorum ID. A single log may provide services for Replicators representing more
 * than one quorum; indeed, LogModule, and OLog behind it, are designed to handle
 * all the quorums on a given server. There's nothing preventing more than one instance
 * of OLog and/or LogModule from running on a given server; it just isn't necessary.
 * <p>
 * In addition, there's nothing preventing more than one JVM instance from hosting
 * OLog instances on a given machine; in fact, all cooperating replicators for a
 * single quorum may all be in one server, one JVM instance, and/or one machine,
 * although from the standpoint of hardware fault tolerance there is no reason to do
 * so. For more information about how quorums are used, see LogModule and
 * {@link c5db.interfaces.ReplicationModule}.
 * <p>
 * OLog's entries are {@link c5db.log.OLogEntry}. These are for the most part
 * agnostic about their payload, which could represent any sort of instructions or
 * objects the user wants to replicate. However, OLogEntry also can represent
 * information needed by Raft itself; for instance, quorum configuration changes
 * are handled by replicating special instances of OLogEntry.
 * <p>
 * QuorumDelegatingLog makes use of {@link c5db.log.LogPersistenceService} to abstract
 * away all the information about the medium to which the log is persisted. At
 * present, c5db.log provides one implementation, {@link c5db.log.LogFileService},
 * which writes the log(s) to local file system files. QuorumDelegatingLog is
 * so named because it gives each quorum its own local log file, and distributes
 * log actions (appends, truncations, rolls) to each as necessary. This simplifies
 * the implementation at the cost of locality of writes. A different implementation
 * of OLog and of LogPersistenceService could choose to put different quorums'
 * entries in the same physical file.
 * <p>
 * The file-like abstraction used internally is
 * {@link c5db.log.LogPersistenceService.BytePersistence}. Operations with the
 * LogPersistenceService are expressed by manipulating BytePersistence instances.
 * As mentioned, LogFileService implements BytePersistence with IO to a file system
 * file.
 * <p>
 * Although "logically" in the Raft algorithm each quorum has a single log continuity,
 * the implementation may distribute that log across several BytePersistence objects.
 * (e.g. in our case, files). That keeps the log file from growing without bound; at
 * some point, the file can be closed and a new file started. This operation is
 * called "rolling". The old file may be stored for some time in case it is needed
 * by either the Raft algorithm or for failure recovery; and eventually when it is
 * no longer needed it can be deleted.
 * <p>
 * {@link c5db.log.EncodedSequentialLog} is the class that handles operations within
 * a single BytePersistence, and therefore also within a single quorum. It can
 * provide iterators over the entries stored within one persistence. These iterators
 * are used by the clients of the replication system in case they need to replay
 * earlier logged data (for instance, for recovery).
 */

package c5db.log;
