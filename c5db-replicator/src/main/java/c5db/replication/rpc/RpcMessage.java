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
import c5db.replication.generated.AppendEntriesReply;
import c5db.replication.generated.PreElectionPoll;
import c5db.replication.generated.PreElectionReply;
import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.generated.RequestVote;
import c5db.replication.generated.RequestVoteReply;
import io.protostuff.Message;

/**
 * Wrap a rpc message, this could/should get serialized to the wire (eg: ReplicationWireMessage)
 * <p/>
 * The subclasses exist so we can properly type the Jetlang channels and be clear about our intentions.
 * <p/>
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

    if (wireMessage.getPreElectionPoll() != null) {
      return wireMessage.getPreElectionPoll();
    }

    if (wireMessage.getPreElectionReply() != null) {
      return wireMessage.getPreElectionReply();
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
        getAppendReplyMessage(),
        getPreElectionPollMessage(),
        getPreElectionReplyMessage()
    );
  }


  public boolean isAppendMessage() {
    return message instanceof AppendEntries;
  }

  public boolean isRequestVoteMessage() {
    return message instanceof RequestVote;
  }

  public boolean isPreElectionPollMessage() {
    return message instanceof PreElectionPoll;
  }

  public boolean isAppendReplyMessage() {
    return message instanceof AppendEntriesReply;
  }

  public boolean isRequestVoteReplyMessage() {
    return message instanceof RequestVoteReply;
  }

  public boolean isPreElectionReplyMessage() {
    return message instanceof PreElectionReply;
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

  public PreElectionPoll getPreElectionPollMessage() {
    if (isPreElectionPollMessage()) {
      return (PreElectionPoll) message;
    }
    return null;
  }

  public PreElectionReply getPreElectionReplyMessage() {
    if (isPreElectionReplyMessage()) {
      return (PreElectionReply) message;
    }
    return null;
  }
}
