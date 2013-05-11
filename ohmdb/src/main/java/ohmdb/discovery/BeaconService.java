/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import ohmdb.codec.UdpProtobufDecoder;
import ohmdb.codec.UdpProtobufEncoder;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ohmdb.discovery.Beacon.Availability;

public class BeaconService extends AbstractService {
    /**
     * Information about a node.
     */
    public static class NodeInfo {
        public final Availability availability;
        public final long lastContactTime;

        public NodeInfo(Availability availability, long lastContactTime) {
            this.availability = availability;
            this.lastContactTime = lastContactTime;
        }

        public NodeInfo(Availability availability) {
            this(availability, System.currentTimeMillis());
        }

        @Override
        public String toString() {
            return availability + " last contact: " + lastContactTime;
        }
    }

    /**
     * Public Fiber API -> RequestChannel replies with the map of nodeId -> NodeInfo for the entire
     * peer set.  The map is immutable, and represents a snapshot.
     */
    public final RequestChannel<Integer, ImmutableMap<String, NodeInfo>> stateRequests = new MemoryRequestChannel<>();

    private static final Logger LOG = LoggerFactory.getLogger(BeaconService.class);

    private final int discoveryPort;
    private final Availability nodeInfoFragment;
    private final NioEventLoopGroup eventLoop;
    private final Map<String, NodeInfo> peers = new HashMap<>();
    private final org.jetlang.channels.Channel<Availability> incomingMessages = new MemoryChannel<>();

    // These should be final, but they are initialized in doStart().
    private Channel broadcastChannel = null;
    private InetSocketAddress sendAddress = null;
    private Bootstrap bootstrap = null;
    private Availability beaconMessage = null;
    private Fiber fiber = null;
    private Callback<Availability> incomingMsgCB = new Callback<Availability>() {
        @Override
        public void onMessage(Availability message) {
            LOG.info("Got incoming message {}", message);
            if (!message.hasNodeId()) {
                LOG.error("Incoming availability message does not have node id, ignoring!");
                return;
            }
            // Always just overwrite what was already there
            // TODO merge old data with new data?
            peers.put(message.getNodeId(), new NodeInfo(message));
        }
    };

    private Callback<Request<Integer, ImmutableMap<String, NodeInfo>>> stateRequestCB =
            new Callback<Request<Integer, ImmutableMap<String, NodeInfo>>>() {

        @Override
        public void onMessage(Request<Integer, ImmutableMap<String, NodeInfo>> message) {
            message.reply(ImmutableMap.copyOf(peers));
        }
    };

    private Runnable beaconSender = new Runnable() {
        @Override
        public void run() {
            if (broadcastChannel == null) {
                LOG.info("Channel not available yet, deferring beacon send");
                return;
            }
            LOG.info("Sending beacon broadcast message to {}", sendAddress);
            broadcastChannel.write(new UdpProtobufEncoder.UdpProtobufMessage(sendAddress, beaconMessage));
        }
    };

    public class BeaconMessageHandler extends ChannelInboundMessageHandlerAdapter<Availability> {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, Availability msg) throws Exception {
            incomingMessages.publish(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.info("Exception, ignoring datagram", cause);
        }
    }

    /**
     *
     * @param discoveryPort the port to send discovery beacons on and to listen to
     * @param nodeInfoFragment node information to broadcast, the local addresses will be filled in
     * @throws InterruptedException
     * @throws SocketException
     */
    public BeaconService(int discoveryPort, Availability nodeInfoFragment) throws InterruptedException, SocketException {
        this.discoveryPort = discoveryPort;
        this.nodeInfoFragment = nodeInfoFragment;

        this.eventLoop = new NioEventLoopGroup(1);
    }

    @Override
    protected void doStart() {
        eventLoop.next().execute(new Runnable() {
            @Override
            public void run() {
                bootstrap = new Bootstrap();
                try {
                    bootstrap.group(eventLoop)
                            .channel(NioDatagramChannel.class)
                            .option(ChannelOption.SO_BROADCAST, true)
                            .option(ChannelOption.SO_REUSEADDR, true)
                            .handler(new ChannelInitializer<DatagramChannel>() {
                                @Override
                                protected void initChannel(DatagramChannel ch) throws Exception {
                                    ChannelPipeline p = ch.pipeline();

                                    p.addLast("protobufDecoder", new UdpProtobufDecoder(Availability.getDefaultInstance()));

                                    p.addLast("protobufEncoder", new UdpProtobufEncoder());

                                    p.addLast("beaconMessageHandler", new BeaconMessageHandler());
                                }
                            });
                    // Wait, this is why we are in a new executor...
                    bootstrap.bind(discoveryPort).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            broadcastChannel = future.channel();
                        }
                    });
                    sendAddress = new InetSocketAddress("255.255.255.255", discoveryPort);
                    Availability.Builder msgBuilder = Availability.newBuilder(nodeInfoFragment);
                    msgBuilder.addAllAddresses(getLocalIPs());
                    beaconMessage = msgBuilder.build();

                    fiber = new ThreadFiber();

                    // Schedule fiber tasks and subscriptions.
                    incomingMessages.subscribe(fiber, incomingMsgCB);
                    stateRequests.subscribe(fiber, stateRequestCB);
                    fiber.scheduleAtFixedRate(beaconSender, 2, 10, TimeUnit.SECONDS);

                    fiber.start();
                } catch (Throwable t) {
                    // we have failed, clean up:
                    bootstrap.shutdown();
                    if (fiber != null)
                        fiber.dispose();

                    notifyFailed(t);
                }
                notifyStarted();
            }
        });
    }

    @Override
    protected void doStop() {
        eventLoop.next().execute(new Runnable() {
            @Override
            public void run() {
                fiber.dispose();
                bootstrap.shutdown();

                notifyStopped();
            }
        });
    }

    private List<String> getLocalIPs() throws SocketException {
        List<String> ips = new LinkedList<>();
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
            NetworkInterface iface = interfaces.nextElement();
            for (Enumeration<InetAddress> addrs = iface.getInetAddresses(); addrs.hasMoreElements(); ) {
                InetAddress addr = addrs.nextElement();
                if (addr.isLoopbackAddress() || addr.isLinkLocalAddress() || addr.isAnyLocalAddress()) {
                    continue;
                }
                ips.add(addr.getHostAddress());
            }
        }
        return ips;
    }
}
