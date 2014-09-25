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
 * An rpc reply in response to a 'wire request'.
 */
public class RpcReply extends RpcMessage {
  /**
   * Invert the to/from and quote the messageId.
   *
   * @param message the reply message
   */
  public RpcReply(Message message) {
    super(0, 0, null, message);
  }
}
