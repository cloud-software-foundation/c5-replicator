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

package c5db.replication;

import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorReceipt;
import c5db.replication.generated.LogEntry;
import com.google.common.util.concurrent.SettableFuture;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a log request, for internal use by ReplicatorInstance.
 */
class InternalReplicationRequest {
  public final List<ByteBuffer> data;
  public final QuorumConfiguration config;
  public final SettableFuture<ReplicatorReceipt> logReceiptFuture;

  public static InternalReplicationRequest toLogData(List<ByteBuffer> data) {
    return new InternalReplicationRequest(data, null);
  }

  public static InternalReplicationRequest toChangeConfig(QuorumConfiguration config) {
    return new InternalReplicationRequest(new ArrayList<>(), config);
  }

  public LogEntry getEntry(long term, long index) {
    return new LogEntry(term, index, data, config == null ? null : config.toProtostuff());
  }

  private InternalReplicationRequest(List<ByteBuffer> data, QuorumConfiguration config) {
    this.data = data;
    this.config = config;
    this.logReceiptFuture = SettableFuture.create();
  }
}
