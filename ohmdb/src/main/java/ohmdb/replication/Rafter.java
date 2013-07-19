package ohmdb.replication;

import com.google.common.collect.ImmutableList;
import ohmdb.replication.rpc.*;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Single instantation of a raft / log / lease
 */
public class Rafter {
    private static final Logger LOG = LoggerFactory.getLogger(Rafter.class);

    private final RequestChannel<RpcRequest,RpcWireReply> sendRpcChannel;
    private final RequestChannel<RpcWireRequest,RpcReply> incomingChannel = new MemoryRequestChannel<>();

    private final Fiber fiber;
    private final long myId;
    private final String quorumId;

    private final ImmutableList<Long> peers;
    private int majority;

    //final InformationInterface info;
    public Rafter(Fiber fiber,
                  long myId,
                  String quorumId,
                  List<Long> peers,
                  RequestChannel<RpcRequest,RpcWireReply> sendRpcChannel) {
        this.fiber = fiber;
        this.myId = myId;
        this.quorumId = quorumId;
        this.peers = ImmutableList.copyOf(peers);
        this.sendRpcChannel = sendRpcChannel;

        assert this.peers.contains(this.myId);

        incomingChannel.subscribe(fiber, new Callback<Request<RpcWireRequest, RpcReply>>() {
            @Override
            public void onMessage(Request<RpcWireRequest, RpcReply> message) {
                onIncomingMessage(message);
            }
        });
    }

    private void onIncomingMessage(Request<RpcWireRequest, RpcReply> message) {
        // TODO this method
    }

}
