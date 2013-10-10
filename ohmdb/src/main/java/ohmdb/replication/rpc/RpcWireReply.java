package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

/**
 * A reply from the wire, a remote agent, replying to a request.
 */
public class RpcWireReply extends RpcMessage {

    public RpcWireReply(long to, long from, long messageId, MessageLite message) {
        super(to, from, messageId, message);
    }
}
