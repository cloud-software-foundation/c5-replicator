package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

public class RpcReply extends RpcMessage {
     /**
      *  Invert the to/from and quote the messageId.
      *
      * @param inReplyTo
      * @param message
      */
    public RpcReply(RpcWireRequest inReplyTo, MessageLite message) {
        super(inReplyTo.from, inReplyTo.to, inReplyTo.messageId, message);
    }
}
