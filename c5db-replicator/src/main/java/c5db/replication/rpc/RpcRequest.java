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


import io.protostuff.Message;

/**
 * An outbound request for the transport.  Since the transport knows who 'we' are, the only
 * params required is a 'to' and which quorumId is being involved.  Oh yes and the actual message.
 * <p/>
 * Actually scratch that, apparently certain types of transports (e.g. in-ram simulations) don't know
 * who 'we' are.  So include that.
 */
public class RpcRequest extends RpcMessage {

  public RpcRequest(long to, long from, String quorumId, Message message) {
    // Note that the RPC system should sub in a message id, that is an implementation detail
    // since not all transports (eg: in RAM only transport) need message IDs to keep request/replies in line.
    super(to, from, quorumId, message);
  }
}

