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

package c5db.interfaces.replication;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A value type for information returned when requesting to replicate data.
 */
public final class ReplicateSubmissionInfo {
  public final long sequenceNumber;
  public final ListenableFuture<Void> completedFuture;

  public ReplicateSubmissionInfo(long sequenceNumber, ListenableFuture<Void> completedFuture) {
    this.sequenceNumber = sequenceNumber;
    this.completedFuture = completedFuture;
  }

  @Override
  public String toString() {
    return "ReplicateSubmissionInfo{" +
        "sequenceNumber=" + sequenceNumber +
        ", completedFuture=" + completedFuture +
        '}';
  }
}
