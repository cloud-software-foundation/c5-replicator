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
package ohmdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.MessageLite;
import ohmdb.discovery.BeaconService;
import ohmdb.messages.ControlMessages;
import org.jetlang.channels.Channel;
import org.jetlang.channels.RequestChannel;

import java.util.concurrent.ExecutionException;

/**
 * Provides bootstrapping and other service introspection and management utilities.  Ideally we can run multiple
 * OhmServer on the same JVM for testing (may be conflicts with the discovery methods).
 */
public interface OhmServer extends Service {
    /***** Interface type public methods ******/

    public long getNodeId();

    public ListenableFuture<OhmService> getServiceByName(String serviceName);

    public Channel<MessageLite> getCommandChannel();

    public RequestChannel<MessageLite, ControlMessages.CommandReply> getCommandRequests();

    public Channel<ServiceStateChange> getServiceRegisteredChannel();

    public ImmutableMap<String, OhmService> getServices() throws ExecutionException, InterruptedException;
    public ListenableFuture<ImmutableMap<String, OhmService>> getServices2();

    public ListenableFuture<BeaconService> getBeaconService();

    public ConfigDirectory getConfigDirectory();

    public static class ServiceStateChange {
        public final OhmService service;
        public final State state;

        @Override
        public String toString() {
            return "ServiceStateChange{" +
                    ", service=" + service +
                    ", state=" + state +
                    '}';
        }

        public ServiceStateChange(OhmService service, State state) {
            this.service = service;
            this.state = state;
        }
    }

    public Channel<ConfigKeyUpdated> getConfigUpdateChannel();
    public static class ConfigKeyUpdated {
        public final String configKey;
        public final Object configValue;

        public ConfigKeyUpdated(String configKey, Object configValue) {
            this.configKey = configKey;
            this.configValue = configValue;
        }

        @Override
        public String toString() {
            return "ConfigKeyUpdated{" +
                    "configKey='" + configKey + '\'' +
                    ", configValue=" + configValue +
                    '}';
        }

    }
}
