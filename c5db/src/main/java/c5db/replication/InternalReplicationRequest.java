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

package c5db.replication;

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
  public final SettableFuture<Long> logNumberNotification;

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
    this.logNumberNotification = SettableFuture.create();
  }
}
