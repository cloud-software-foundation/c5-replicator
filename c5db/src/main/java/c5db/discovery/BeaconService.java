/*
 * Copyright (C) 2014  Ohm Data
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
package c5db.discovery;

import c5db.codec.UdpProtostuffDecoder;
import c5db.codec.UdpProtostuffEncoder;
import c5db.discovery.generated.Availability;
import c5db.discovery.generated.ModuleDescriptor;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.messages.generated.ModuleType;
import c5db.util.FiberOnly;
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
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
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


public class BeaconService extends AbstractService implements DiscoveryModule {
  private static final Logger LOG = LoggerFactory.getLogger(BeaconService.class);
  public static final String LOOPBACK_ADDRESS = "127.0.0.1";
  public static final String BROADCAST_ADDRESS = "255.255.255.255";

  @Override
    public ModuleType getModuleType() {
        return ModuleType.Discovery;
    }

    @Override
    public boolean hasPort() {
        return true;
    }

    @Override
    public int port() {
        return discoveryPort;
    }

    private final RequestChannel<NodeInfoRequest, NodeInfoReply> nodeInfoRequests = new MemoryRequestChannel<>();
    @Override
    public RequestChannel<NodeInfoRequest, NodeInfoReply> getNodeInfo() {
        return nodeInfoRequests;
    }
    @FiberOnly
    private void handleNodeInfoRequest(Request<NodeInfoRequest, NodeInfoReply> message) {
        NodeInfoRequest req = message.getRequest();
        NodeInfo peer = peers.get(req.nodeId);
        if (peer == null) {
            message.reply(NodeInfoReply.NO_REPLY);
            return;
        }

        Integer servicePort = peer.modules.get(req.moduleType);
        if (servicePort == null) {
            message.reply(NodeInfoReply.NO_REPLY);
            return;
        }

        List<String> peerAddrs = peer.availability.getAddressesList();
        // does this module run on that peer?
        message.reply(new NodeInfoReply(true, peerAddrs, servicePort));
    }

    @Override
    public String toString() {
        return "BeaconService{" +
                "discoveryPort=" + discoveryPort +
                ", nodeId=" + nodeId +
                '}';
    }

    // For main system modules/pubsub stuff.
    private final C5Server c5Server;
    private final long nodeId;
    private final int discoveryPort;
    private final NioEventLoopGroup eventLoop;
    private final Map<ModuleType, Integer> moduleInfo = new HashMap<>();
    private final Map<Long, NodeInfo> peers = new HashMap<>();
    private final org.jetlang.channels.Channel<Availability> incomingMessages = new MemoryChannel<>();
    private final Fiber fiber;

    // These should be final, but they are initialized in doStart().
    private Channel broadcastChannel = null;
    private InetSocketAddress sendAddress = null;
    private Bootstrap bootstrap = null;
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
    public BeaconService(long nodeId,
                         int discoveryPort,
                         final Fiber fiber,
                         NioEventLoopGroup eventLoop,
                         Map<ModuleType, Integer> modules,
                         C5Server theC5Server
                         ) throws InterruptedException, SocketException {
        this.discoveryPort = discoveryPort;
        this.nodeId = nodeId;
        this.fiber = fiber;
        moduleInfo.putAll(modules);
        this.c5Server = theC5Server;
        this.eventLoop = eventLoop;
    }

    @Override
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

    org.jetlang.channels.Channel<NewNodeVisible> newNodeVisibleChannel = new MemoryChannel<>();
    @Override
    public org.jetlang.channels.Channel<NewNodeVisible> getNewNodeNotifications() {
        return newNodeVisibleChannel;
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
        LOG.trace("Sending beacon broadcast message to {}", sendAddress);

        List<ModuleDescriptor> msgModules = new ArrayList<>(moduleInfo.size());
        for (ModuleType moduleType : moduleInfo.keySet()) {
            msgModules.add(
                    new ModuleDescriptor(moduleType,
                            moduleInfo.get(moduleType)));
        }

        Availability beaconMessage = new Availability(nodeId, 0, localIPs, msgModules);

        broadcastChannel.writeAndFlush(new
            UdpProtostuffEncoder.UdpProtostuffMessage<>(sendAddress, beaconMessage));

        // Fix issue #76, feed back the beacon Message to our own database:
        processWireMessage(beaconMessage);
    }

    @FiberOnly
    private void processWireMessage(Availability message) {
        LOG.trace("Got incoming message {}", message);
        if (message.getNodeId() == 0) {
//        if (!message.hasNodeId()) {
            LOG.error("Incoming availability message does not have node id, ignoring!");
            return;
        }
        // Always just overwrite what was already there for now.
        // TODO consider a more sophisticated merge strategy?
        NodeInfo nodeInfo = new NodeInfo(message);
        if (!peers.containsKey(message.getNodeId())) {
            getNewNodeNotifications().publish(new NewNodeVisible(message.getNodeId(), nodeInfo));
        }

        peers.put(message.getNodeId(), nodeInfo);
    }

    @FiberOnly
    private void serviceChange(C5Server.ModuleStateChange message) {
        if (message.state == State.RUNNING) {
            LOG.debug("BeaconService adding running module {} on port {}",
                    message.module.getModuleType(),
                    message.module.port());
            moduleInfo.put(message.module.getModuleType(), message.module.port());
        } else if (message.state == State.STOPPING || message.state == State.FAILED || message.state == State.TERMINATED) {
            LOG.debug("BeaconService removed module {} on port {} with state {}",
                    message.module.getModuleType(),
                    message.module.port(),
                    message.state);
            moduleInfo.remove(message.module.getModuleType());
        } else {
            LOG.debug("BeaconService got unknown state module change {}", message);
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

                                    p.addLast("protobufDecoder",
                                            new UdpProtostuffDecoder<>(Availability.getSchema(), false));

                                    p.addLast("protobufEncoder",
                                            new UdpProtostuffEncoder<>(Availability.getSchema(), false));

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
                    if (System.getProperties().containsKey("singleNode")){
                      sendAddress = new InetSocketAddress(LOOPBACK_ADDRESS, discoveryPort);
                    } else {
                      sendAddress = new InetSocketAddress(BROADCAST_ADDRESS, discoveryPort);
                    }
                    //Availability.Builder msgBuilder = Availability.newBuilder(nodeInfoFragment);
                    localIPs = getLocalIPs();
                    //msgBuilder.addAllAddresses(getLocalIPs());
                    //beaconMessage = msgBuilder.build();

                    // Schedule fiber tasks and subscriptions.
                    incomingMessages.subscribe(fiber, new Callback<Availability>() {
                        @Override
                        public void onMessage(Availability message) {
                            processWireMessage(message);
                        }
                    });
                    nodeInfoRequests.subscribe(fiber, new Callback<Request<NodeInfoRequest, NodeInfoReply>>() {
                        @Override
                        public void onMessage(Request<NodeInfoRequest, NodeInfoReply> message) {
                            handleNodeInfoRequest(message);
                        }
                    });

                    fiber.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            sendBeacon();
                        }
                    }, 2, 10, TimeUnit.SECONDS);

                    c5Server.getModuleStateChangeChannel().subscribe(fiber, new Callback<C5Server.ModuleStateChange>() {
                        @Override
                        public void onMessage(C5Server.ModuleStateChange message) {
                            serviceChange(message);
                        }
                    });

                    fiber.start();

                    notifyStarted();
                } catch (Throwable t) {
                    if (fiber != null)
                        fiber.dispose();

                    notifyFailed(t);
                }
            }
        });
    }

    @Override
    protected void doStop() {
        eventLoop.next().execute(new Runnable() {
            @Override
            public void run() {
                fiber.dispose();

                notifyStopped();
            }
        });
    }

    private List<String> getLocalIPs() throws SocketException {
        List<String> ips = new LinkedList<>();
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
            NetworkInterface iface = interfaces.nextElement();
            if (iface.isPointToPoint())
                continue; //ignore tunnel type interfaces
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
