package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

/**
 * RPC Requests from the client => dont need a message id.
 * RPC Requests from the WIRE => message id already comes with.
 *
 * An outbound request for the sender subsystem.
 */
public class RpcRequest extends RpcMessage {

    public RpcRequest(long to, long from, MessageLite message) {
        // Note that the RPC system should sub in a message id, that is an implementation detail
        // since not all transports (eg: in RAM only transport) need message IDs to keep request/replies in line.
        super(to, from, 0, message);
    }
}

