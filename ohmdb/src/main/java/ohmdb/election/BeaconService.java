package ohmdb.election;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

public class BeaconService extends AbstractExecutionThreadService {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconService.class);

    private final Channel broadcastChannel;
    private final InetSocketAddress sendAddress;
    private final Gossip.Availability beaconMessage;
    private Thread theThread;

    public BeaconService(final Channel broadcastChannel, Gossip.Availability nodeInfoFragment) throws SocketException {
        this.broadcastChannel = broadcastChannel;
        int port = ((InetSocketAddress)broadcastChannel.localAddress()).getPort();
        this.sendAddress = new InetSocketAddress("255.255.255.255", port);

        // TODO reread the local addresses on some kind of ... ?
        Gossip.Availability.Builder b = Gossip.Availability.newBuilder(nodeInfoFragment);
        b.addAllAddresses(getLocalIPs());
        this.beaconMessage = b.build();
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

    @Override
    protected void triggerShutdown() {
        theThread.interrupt();
    }

    @Override
    protected void run() throws Exception {
        this.theThread = Thread.currentThread();
        while(isRunning()) {

            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                LOG.info("I was interrupted from sleep, annoying");
                continue;
            }
            // Allow the throwable to escape to fail the service if anything fails.
            LOG.info("Sending beacon broadcast message to {}", sendAddress);
            broadcastChannel.write(new UdpProtobufEncoder.UdpProtobufMessage(sendAddress, beaconMessage)).sync();
        }
    }
}
