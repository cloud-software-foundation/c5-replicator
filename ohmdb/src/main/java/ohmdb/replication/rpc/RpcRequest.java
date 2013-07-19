package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

/**
 * RPC Requests from the client => dont need a message id.
 * RPC Requests from the WIRE => message id already comes with.
 */
public class RpcRequest extends RpcMessage {

    public RpcRequest(long to, long from, MessageLite message) {
        super(to, from, 0, message);
    }
}

