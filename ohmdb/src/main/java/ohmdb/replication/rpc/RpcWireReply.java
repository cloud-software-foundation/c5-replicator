package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

public class RpcWireReply extends RpcMessage {

    public RpcWireReply(long to, long from, long messageId, MessageLite message) {
        super(to, from, messageId, message);
    }
}
