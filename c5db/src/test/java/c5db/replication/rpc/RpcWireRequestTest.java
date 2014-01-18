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


import c5db.replication.generated.Raft;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**

 */
public class RpcWireRequestTest {
    @Test
    public void testGetSubMsg() throws Exception {
        Raft.RaftWireMessage wireMessage = Raft.RaftWireMessage.newBuilder()
                .setMessageId(1)
                .setSenderId(42)
                .setReceiverId(42)
                .setRequestVote(Raft.RequestVote.newBuilder()
                        .setCandidateId(1)
                        .setLastLogIndex(22)
                        .setLastLogTerm(33)
                        .setTerm(33))
                .build();

        RpcWireRequest rpcMsg = new RpcWireRequest(wireMessage);
        assertTrue(rpcMsg.message instanceof Raft.RequestVote);
        assertEquals(42, rpcMsg.to);
        assertEquals(42, rpcMsg.from);
    }
}
