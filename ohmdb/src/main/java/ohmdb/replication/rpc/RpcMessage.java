package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

/**
 * Wrap a rpc message.
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
}
