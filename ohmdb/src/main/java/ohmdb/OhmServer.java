package ohmdb;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.MessageLite;
import ohmdb.messages.ControlMessages;
import org.jetlang.channels.Channel;
import org.jetlang.channels.RequestChannel;

/**
 * Provides bootstrapping and other service introspection and management utilities.  Ideally we can run multiple
 * OhmServer on the same JVM for testing (may be conflicts with the discovery methods).
 */
public interface OhmServer extends Service {
    /***** Interface type public methods ******/

    long getNodeId();

    ListenableFuture<OhmService> getServiceByName(String serviceName);

    Channel<MessageLite> getCommandChannel();

    RequestChannel<MessageLite, ControlMessages.CommandReply> getCommandRequests();

    Channel<ServiceStateChange> getServiceRegisteredChannel();

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
        public final State state;

        public ServiceStateChange(String serviceName, int port, State state) {
            this.serviceName = serviceName;
            this.port = port;
            this.state = state;
        }
    }
}
