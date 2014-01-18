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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**

 */
public class RpcMessageTest {
    @Test
    public void testGetWireMessageFragment() throws Exception {
        Raft.RequestVote requestVote = Raft.RequestVote.newBuilder()
                .setCandidateId(1)
                .setLastLogIndex(22)
                .setLastLogTerm(42)
                .setTerm(43)
                .build();

        RpcMessage msg = new RpcMessage(0, 0, "quorumId", requestVote);

        Raft.RaftWireMessage.Builder wireMessage = msg.getWireMessageFragment();
        assertTrue(wireMessage.hasRequestVote());

        assertFalse(wireMessage.hasAppendEntries());
        assertFalse(wireMessage.hasAppendEntriesReply());
    }

    @Test
    public void testGetWireMessageFragment2() throws Exception {

        Raft.AppendEntries appendEntries = Raft.AppendEntries.newBuilder()
                .setCommitIndex(111)
                .setLeaderId(1)
                .setPrevLogIndex(2)
                .build();

        RpcMessage msg = new RpcMessage(0, 0, "quorumId", appendEntries);
        Raft.RaftWireMessage.Builder wireMessage = msg.getWireMessageFragment();
        assertTrue(wireMessage.hasAppendEntries());
        assertFalse(wireMessage.hasAppendEntriesReply());
        assertFalse(wireMessage.hasRequestVote());
        assertFalse(wireMessage.hasRequestVoteReply());
    }
}
