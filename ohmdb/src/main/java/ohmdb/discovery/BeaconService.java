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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import ohmdb.Server;
import ohmdb.codec.UdpProtobufDecoder;
import ohmdb.codec.UdpProtobufEncoder;
import ohmdb.util.FiberOnly;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ohmdb.discovery.Beacon.Availability;

public class BeaconService extends AbstractService {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconService.class);


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

    // For main system services/pubsub stuff.
    private final Server server;
    private final long nodeId;
    private final int discoveryPort;
    private final NioEventLoopGroup eventLoop;
    private final Map<String, Integer> serviceInfo = new HashMap<>();
    private final Map<Long, NodeInfo> peers = new HashMap<>();
    private final org.jetlang.channels.Channel<Availability> incomingMessages = new MemoryChannel<>();

    // These should be final, but they are initialized in doStart().
    private Channel broadcastChannel = null;
    private InetSocketAddress sendAddress = null;
    private Bootstrap bootstrap = null;
    private Fiber fiber = null;
    private List<String> localIPs;

    public class BeaconMessageHandler extends SimpleChannelInboundHandler<Availability> {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.warn("Exception, ignoring datagram", cause);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Availability msg) throws Exception {
            incomingMessages.publish(msg);
        }
    }

    /**
     *
     * @param nodeId the id of this node.
     * @param discoveryPort the port to send discovery beacons on and to listen to
     * @throws InterruptedException
     * @throws SocketException
     */
    public BeaconService(long nodeId, int discoveryPort,
                         Map<String, Integer> services,
                         Server theServer
                         ) throws InterruptedException, SocketException {
        this.discoveryPort = discoveryPort;
        this.nodeId = nodeId;
        serviceInfo.putAll(services);
        this.server = theServer;

        this.eventLoop = new NioEventLoopGroup(1);
    }

    public ListenableFuture<ImmutableMap<Long, NodeInfo>> getState() {
        final SettableFuture<ImmutableMap<Long,NodeInfo>> future = SettableFuture.create();

        fiber.execute(new Runnable() {
            @Override
            public void run() {
                future.set(getCopyOfState());
            }
        });

        return future;
    }

    private ImmutableMap<Long, NodeInfo> getCopyOfState() {
        return ImmutableMap.copyOf(peers);
    }

    @FiberOnly
    private void sendBeacon() {
        if (broadcastChannel == null) {
            LOG.debug("Channel not available yet, deferring beacon send");
            return;
        }
        LOG.debug("Sending beacon broadcast message to {}", sendAddress);
        // Build beacon message:
        Availability.Builder beaconMessage = Availability.newBuilder()
                .addAllAddresses(localIPs)
                .setNodeId(nodeId);

        List<Beacon.ServiceDescriptor> msgServices = new ArrayList<>(serviceInfo.size());
        for (String svcName : serviceInfo.keySet()) {
            msgServices.add(Beacon.ServiceDescriptor.newBuilder()
            .setServiceName(svcName)
            .setServicePort(serviceInfo.get(svcName))
            .build());
        }

        beaconMessage.addAllServices(msgServices);

        broadcastChannel.writeAndFlush(new UdpProtobufEncoder.UdpProtobufMessage(sendAddress, beaconMessage));
    }

    @FiberOnly
    private void processWireMessage(Availability message) {
        LOG.debug("Got incoming message {}", message);
        if (!message.hasNodeId()) {
            LOG.error("Incoming availability message does not have node id, ignoring!");
            return;
        }
        // Always just overwrite what was already there
        // TODO merge old data with new data?
        peers.put(message.getNodeId(), new NodeInfo(message));
    }

    @FiberOnly
    private void serviceChange(Server.ServiceRegistered message) {
        if (message.state == State.RUNNING) {
            LOG.debug("BeaconService adding running service {} on port {}", message.serviceName, message.port);
            serviceInfo.put(message.serviceName, message.port);
        } else if (message.state == State.STOPPING || message.state == State.FAILED || message.state == State.TERMINATED) {
            LOG.debug("BeaconService removed service {} on port {} with state {}", message.serviceName, message.port,
                    message.state);
            serviceInfo.remove(message.serviceName);
        } else {
            LOG.debug("BeaconService got unknown service change {}", message);
        }
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
                    //Availability.Builder msgBuilder = Availability.newBuilder(nodeInfoFragment);
                    localIPs = getLocalIPs();
                    //msgBuilder.addAllAddresses(getLocalIPs());
                    //beaconMessage = msgBuilder.build();

                    fiber = new ThreadFiber();

                    // Schedule fiber tasks and subscriptions.
                    incomingMessages.subscribe(fiber, new Callback<Availability>() {
                        @Override
                        public void onMessage(Availability message) {
                            processWireMessage(message);
                        }
                    });

                    fiber.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            sendBeacon();
                        }
                    }, 2, 10, TimeUnit.SECONDS);

                    server.getServiceRegisteredChannel().subscribe(fiber, new Callback<Server.ServiceRegistered>() {
                        @Override
                        public void onMessage(Server.ServiceRegistered message) {
                            serviceChange(message);
                        }
                    });

                    fiber.start();
                } catch (Throwable t) {
                    // we have failed, clean up:
                    //bootstrap.;
                    bootstrap.group().shutdownGracefully();
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
                bootstrap.group().shutdownGracefully();

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
