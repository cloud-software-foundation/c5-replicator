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


import c5db.replication.generated.AppendEntries;
import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.generated.RequestVote;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**

 */
public class RpcMessageTest {
  @Test
  public void testGetWireMessageFragment() throws Exception {
    RequestVote requestVote = new RequestVote(22, 1, 42, 43);

    RpcMessage msg = new RpcMessage(0, 0, "quorumId", requestVote);

    ReplicationWireMessage wireMessage = msg.getWireMessage(1, 1, 1, false);
    assertNotEquals(null, wireMessage.getRequestVote());
    assertEquals(null, wireMessage.getAppendEntries());
    assertEquals(null, wireMessage.getAppendEntriesReply());
  }

  @Test
  public void testGetWireMessageFragment2() throws Exception {

    AppendEntries appendEntries = new AppendEntries(111, 1, 200, 201,
        Collections.emptyList(),
        111);
    RpcMessage msg = new RpcMessage(0, 0, "quorumId", appendEntries);
    ReplicationWireMessage wireMessage = msg.getWireMessage(1, 1, 1, false);
    assertNotEquals(null, wireMessage.getAppendEntries());
    assertEquals(null, wireMessage.getAppendEntriesReply());
    assertEquals(null, wireMessage.getRequestVote());
    assertEquals(null, wireMessage.getRequestVoteReply());
  }
}
