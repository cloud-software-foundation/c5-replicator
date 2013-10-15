/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;
import ohmdb.replication.Raft;

/**
 * Wrap a rpc message, this could/should get serialized to the wire (eg: RaftWireMessage)
 *
 * The subclasses exist so we can properly type the Jetlang channels and be clear about our intentions.
 *
 * There is 4 subclasses in 2 categories:
 * * Wire   -- for messages off the wire from other folks
 * * Non-wire  -- for messages to be sent to other folks
 *
 *
 */
public class RpcMessage {
    public final long to;
    public final long from;
    public final long messageId;

    public final MessageLite message;

    public RpcMessage(long to, long from, long messageId, MessageLite message) {
        this.to = to;
        this.from = from;
        this.messageId = messageId;

        this.message = message;
    }

    @Override
    public String toString() {
        return String.format("From: %d to: %d message: %d contents: %s", from, to, messageId, message);
    }

    public boolean isAppendMessage() {
        return message instanceof Raft.AppendEntries;
    }
    public boolean isRequestVoteMessage() {
        return message instanceof Raft.RequestVote;
    }
    public boolean isAppendReplyMessage() {
        return message instanceof Raft.AppendEntriesReply;
    }
    public boolean isRequestVoteReplyMessage() {
        return message instanceof Raft.RequestVoteReply;
    }

    public String getQuorumId() {
        if (isAppendMessage())
            return getAppendMessage().getQuorumId();
        if (isAppendReplyMessage())
            return getAppendReplyMessage().getQuorumId();
        if (isRequestVoteMessage())
            return getRequestVoteMessage().getQuorumId();
        if (isRequestVoteReplyMessage())
            return getRequestVoteReplyMessage().getQuorumId();
        throw new RuntimeException("Unknown message type is impossible");
    }

    public Raft.AppendEntries getAppendMessage() {
        assert isAppendMessage();

        return (Raft.AppendEntries) message;
    }
    public Raft.AppendEntriesReply getAppendReplyMessage() {
        assert isAppendReplyMessage();

        return (Raft.AppendEntriesReply) message;
    }

    public Raft.RequestVote getRequestVoteMessage() {
        assert isRequestVoteMessage();

        return (Raft.RequestVote) message;
    }

    public Raft.RequestVoteReply getRequestVoteReplyMessage() {
        assert isRequestVoteReplyMessage();

        return (Raft.RequestVoteReply) message;
    }
}
