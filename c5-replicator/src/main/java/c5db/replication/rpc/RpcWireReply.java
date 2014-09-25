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

package c5db.replication.rpc;

import c5db.replication.generated.ReplicationWireMessage;
import io.protostuff.Message;

/**
 * A reply from the wire - a remote agent - replying to a request.
 */
public class RpcWireReply extends RpcMessage {

  public RpcWireReply(long to, long from, String quorumId, Message message) {
    super(to, from, quorumId, message);
  }

  public RpcWireReply(ReplicationWireMessage msg) {
    super(msg);
  }
}
