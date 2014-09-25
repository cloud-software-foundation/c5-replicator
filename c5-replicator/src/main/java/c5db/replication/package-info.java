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
 * This package contains functionality for replicating logs. A log in this context is any sequence of data
 * items; for instance, a write-ahead log. "Replication" here means functionally duplicating the information
 * in the log, in general on several different durable media hosted by different machines. The purposes of
 * replication are fault tolerance and availability.
 * <p>
 * The package contains an implementation of the {@link c5db.interfaces.ReplicationModule} interface:
 * ReplicatorService. A given ReplicatorService instance participates in the replication of zero to
 * many logs.
 * <p>
 * ReplicatorService keeps a separate instance of the implementation of the
 * {@link c5db.interfaces.replication.Replicator} interface for each quorum it participates in. Each such
 * implementation (e.g. ReplicatorInstance) is in charge of the logic and state for the replication of that
 * quorum, and that logic and state is separate from that of any other quorum supported by the same
 * ReplicatorService.
 */

package c5db.replication;
