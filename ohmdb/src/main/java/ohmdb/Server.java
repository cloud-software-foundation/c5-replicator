package ohmdb;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.MessageLite;
import ohmdb.discovery.BeaconService;
import ohmdb.util.FiberOnly;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static ohmdb.messages.ControlMessages.CommandReply;
import static ohmdb.messages.ControlMessages.StartService;
import static ohmdb.messages.ControlMessages.StopService;


/**
 * Holds information about all other services, can start/stop other services, etc.
 * Knows the 'root' information about this server as well, such as NodeId, etc.
 *
 * To shut down the 'server' service is to shut down the server.
 */
public class Server extends AbstractService {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    public void main(String[] args) {
        // TODO parse those command line params. How tedious.
        instance = new Server(1);
        instance.start();
    }

    private static Server instance = null;

    public Server(long nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Returns the server, but it will be null if you aren't running inside one.
     * @return
     */
    public static Server getServer() {
        return instance;
    }

    /***** Interface type public methods ******/

    public long getNodeId() {
        return nodeId;
    }

//    public Service getServiceByName(String serviceName) {
        // do this on the fiber:
//        return serviceRegistry.get(serviceName);
//
//    }

    public ListenableFuture<Service> getServiceByName(final String serviceName) {
        final SettableFuture<Service> future = SettableFuture.create();
        serverFiber.execute(new Runnable() {
            @Override
            public void run() {
                future.set(serviceRegistry.get(serviceName));
            }
        });
        return future;
    }

    /**** Implementation ****/
    Fiber serverFiber;

    // The mapping between service name and the instance.
    private final Map<String,Service> serviceRegistry = new HashMap<>();

    private final long nodeId;

    private final Channel<MessageLite> commandChannel = new MemoryChannel<>();

    public Channel<MessageLite> getCommandChannel() {
        return commandChannel;
    }

    public RequestChannel<MessageLite, CommandReply> commandRequests = new MemoryRequestChannel<>();
    public RequestChannel<MessageLite, CommandReply> getCommandRequests() {
        return commandRequests;
    }

    public static class ServiceStateChange {
        @Override
        public String toString() {
            return "ServiceRegistered{" +
                    "serviceName='" + serviceName + '\'' +
                    ", port=" + port +
                    ", state=" + state +
                    '}';
        }

        public final String serviceName;
        public final int port;
        public final Service.State state;

        public ServiceStateChange(String serviceName, int port, State state) {
            this.serviceName = serviceName;
            this.port = port;
            this.state = state;
        }
    }

    private final Channel<ServiceStateChange> serviceRegisteredChannel = new MemoryChannel<>();
    public Channel<ServiceStateChange> getServiceRegisteredChannel() {
        return serviceRegisteredChannel;
    }


    @FiberOnly
    private void processCommandMessage(MessageLite msg) throws Exception {
        if (msg instanceof StartService) {
            StartService message = (StartService) msg;
            startService(message.getServiceName(), message.getServicePort(), message.getServiceArgv());
        }
        else if (msg instanceof StopService) {
            StopService message = (StopService)msg;

            stopService(message.getServiceName(), message.getHardStop(), message.getStopReason());
        }
    }

    private void processCommandRequest(Request<MessageLite, CommandReply> request) {
        MessageLite r = request.getRequest();
        try {
            String stdout = "";

            if (r instanceof StartService) {
                StartService message = (StartService)r;
                startService(message.getServiceName(), message.getServicePort(), message.getServiceArgv());

                stdout = String.format("Service %s started", message.getServiceName());
            } else if (r instanceof StopService) {
                StopService message = (StopService)r;

                stopService(message.getServiceName(), message.getHardStop(), message.getStopReason());

                stdout = String.format("Service %s started", message.getServiceName());
            } else {
                CommandReply reply = CommandReply.newBuilder()
                        .setCommandSuccess(false)
                        .setCommandStderr(String.format("Unknown message type: %s", r.getClass()))
                        .build();
                request.reply(reply);
                return;
            }

            CommandReply reply = CommandReply.newBuilder()
                    .setCommandSuccess(true)
                    .setCommandStdout(stdout)
                    .build();
            request.reply(reply);

        } catch (Exception e) {
            CommandReply reply = CommandReply.newBuilder()
                    .setCommandSuccess(false)
                    .setCommandStderr(e.toString())
                    .build();
            request.reply(reply);
        }
    }

    private class ServiceListenerPublisher implements Listener {
        private final String serviceName;
        private final int servicePort;

        public ServiceListenerPublisher(String serviceName, int servicePort) {
            this.serviceName = serviceName;
            this.servicePort = servicePort;
        }

        @Override
        public void starting() {
            publishEvent(State.STARTING);
        }

        @Override
        public void running() {
            publishEvent(State.RUNNING);
        }

        @Override
        public void stopping(State from) {
            publishEvent(State.STOPPING);
        }

        @Override
        public void terminated(State from) {
            // TODO move this into a subscriber of ourselves.
            serviceRegistry.remove(serviceName);
            publishEvent(State.TERMINATED);
        }

        @Override
        public void failed(State from, Throwable failure) {
            publishEvent(State.FAILED);
        }

        private void publishEvent(State state) {
            ServiceStateChange p = new ServiceStateChange(serviceName, servicePort, state);
            getServiceRegisteredChannel().publish(p);
        }

    }

    @FiberOnly
    private boolean startService(final String serviceName, final int servicePort, String serviceArgv) throws Exception {
        Service s;
        if (serviceName.equals("BeaconService")) {
            Map<String, Integer> l = new HashMap<>();
            for (String name : serviceRegistry.keySet()) {
                l.put(name, 1);
            }

            s = new BeaconService(servicePort, servicePort, l, this);
            s.addListener(new ServiceListenerPublisher(serviceName, servicePort), serverFiber);

            s.start();
            serviceRegistry.put(serviceName, s);
        } else {
            throw new Exception("No such service as " + serviceName);
        }

        return true;
    }

    @FiberOnly
    private void stopService(String serviceName, boolean hardStop, String stopReason) {
        Service theService = serviceRegistry.get(serviceName);
        if (theService == null) {
            LOG.debug("Cant stop service {}, not in registry", serviceName);
            return ;
        }

        theService.stop();
    }

    @Override
    protected void doStart() {
        // Read base state/config from disk.

        // Start basic service set here.

        serverFiber = new ThreadFiber(new RunnableExecutorImpl(), "OhmDb-Server", false);
        commandChannel.subscribe(serverFiber, new Callback<MessageLite>() {
            @Override
            public void onMessage(MessageLite message) {
                try {
                    processCommandMessage(message);
                } catch (Exception e) {
                    LOG.warn("exception during message processing", e);
                }
            }
        });

        commandRequests.subscribe(serverFiber, new Callback<Request<MessageLite, CommandReply>>() {
            @Override
            public void onMessage(Request<MessageLite, CommandReply> request) {
                processCommandRequest(request);
            }
        });

        serverFiber.start();
    }


    @Override
    protected void doStop() {

        // stop service set.

        // write any last minute persistent data to disk (is there any?)

        serverFiber.dispose();
    }

}
