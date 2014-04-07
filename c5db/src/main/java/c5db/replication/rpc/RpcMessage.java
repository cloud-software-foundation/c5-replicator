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
import c5db.replication.generated.AppendEntriesReply;
import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.generated.RequestVote;
import c5db.replication.generated.RequestVoteReply;
import io.protostuff.Message;

/**
 * Wrap a rpc message, this could/should get serialized to the wire (eg: ReplicationWireMessage)
 * <p>
 * The subclasses exist so we can properly type the Jetlang channels and be clear about our intentions.
 * <p>
 * There is 4 subclasses in 2 categories:
 * * Wire   -- for messages off the wire from other folks
 * * Non-wire  -- for messages to be sent to other folks
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

  protected RpcMessage(ReplicationWireMessage wireMessage) {
    this(wireMessage.getReceiverId(),
        wireMessage.getSenderId(),
        wireMessage.getQuorumId(),
        getSubMsg(wireMessage));
  }

  static Message getSubMsg(ReplicationWireMessage wireMessage) {
    if (wireMessage.getAppendEntries() != null) {
      return wireMessage.getAppendEntries();
    }

    if (wireMessage.getAppendEntriesReply() != null) {
      return wireMessage.getAppendEntriesReply();
    }

    if (wireMessage.getRequestVote() != null) {
      return wireMessage.getRequestVote();
    }

    if (wireMessage.getRequestVoteReply() != null) {
      return wireMessage.getRequestVoteReply();
    }

    return null;
  }

  @Override
  public String toString() {
    return String.format("From: %d to: %d message: %s contents: %s", from, to, quorumId, message);
  }

  public ReplicationWireMessage getWireMessage(
      long messageId,
      long from,
      long to,
      boolean inReply
  ) {
    return new ReplicationWireMessage(
        messageId,
        from,
        to,
        quorumId,
        inReply,
        getRequestVoteMessage(),
        getRequestVoteReplyMessage(),
        getAppendMessage(),
        getAppendReplyMessage()
    );
  }


  public boolean isAppendMessage() {
    return message instanceof AppendEntries;
  }

  public boolean isRequestVoteMessage() {
    return message instanceof RequestVote;
  }

  public boolean isAppendReplyMessage() {
    return message instanceof AppendEntriesReply;
  }

  public boolean isRequestVoteReplyMessage() {
    return message instanceof RequestVoteReply;
  }

  public AppendEntries getAppendMessage() {
    if (isAppendMessage()) {
      return (AppendEntries) message;
    }
    return null;
  }

  public AppendEntriesReply getAppendReplyMessage() {
    if (isAppendReplyMessage()) {
      return (AppendEntriesReply) message;
    }
    return null;
  }

  public RequestVote getRequestVoteMessage() {
    if (isRequestVoteMessage()) {
      return (RequestVote) message;
    }
    return null;
  }

  public RequestVoteReply getRequestVoteReplyMessage() {
    if (isRequestVoteReplyMessage()) {
      return (RequestVoteReply) message;
    }
    return null;
  }
}
