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
