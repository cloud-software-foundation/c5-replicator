package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

/**
 * And RPC request from off the wire, from a remote sender.
 */
public class RpcWireRequest extends RpcMessage {

    public RpcWireRequest(long to, long from, long messageId, MessageLite message) {
        super(to, from, messageId, message);
    }

}

