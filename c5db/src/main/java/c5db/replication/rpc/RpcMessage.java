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
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Map;

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
    public final String quorumId;

    public final Message message;

    protected RpcMessage(long to, long from, String quorumId, Message message) {
        this.to = to;
        this.from = from;
        this.quorumId = quorumId;

        this.message = message;
    }

    protected RpcMessage(Raft.RaftWireMessage wireMessage) {
        this(wireMessage.getReceiverId(), wireMessage.getSenderId(), wireMessage.getQuorumId(), getSubMsg(wireMessage));
    }

    static Message getSubMsg(Raft.RaftWireMessage wireMessage) {
        Map<Descriptors.FieldDescriptor, Object> fields = wireMessage.getAllFields();

        for (Descriptors.FieldDescriptor fd : fields.keySet()) {
            if (fd.getNumber() >= 100 && fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                return (Message) wireMessage.getField(fd);
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return String.format("From: %d to: %d message: %s contents: %s", from, to, quorumId, message);
    }

    /**
     * Returns a builder that has only the 'message' part set.  Since the to/from/quorumId may or may not
     * be actually set in this class, we can't really rely on it now.
     *
     * @return a builder that forms the basis of the rest of the message.
     */
    public Raft.RaftWireMessage.Builder getWireMessageFragment() {
        Raft.RaftWireMessage.Builder builder = Raft.RaftWireMessage.newBuilder();

        Descriptors.Descriptor d = Raft.RaftWireMessage.getDescriptor();
        // Note, the structure/class name must match the field name.  Kind of ugly but.
        Descriptors.FieldDescriptor fd = d.findFieldByName(message.getDescriptorForType().getName());
        builder.setField(fd, message);

        return builder;
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
