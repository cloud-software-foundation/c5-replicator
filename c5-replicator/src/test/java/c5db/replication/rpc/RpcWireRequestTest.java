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
import c5db.replication.generated.RequestVote;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**

 */
public class RpcWireRequestTest {
  @Test
  public void testGetSubMsg() throws Exception {
    ReplicationWireMessage wireMessage = new ReplicationWireMessage(
        1, 42, 42, "quorum", false,
        new RequestVote(33, 1, 22, 33),
        null, null, null, null, null
    );

    RpcWireRequest rpcMsg = new RpcWireRequest(wireMessage);
    assertTrue(rpcMsg.message instanceof RequestVote);
    assertEquals(42, rpcMsg.to);
    assertEquals(42, rpcMsg.from);
  }
}
