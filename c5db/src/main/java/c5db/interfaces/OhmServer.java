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
package c5db.interfaces;

import c5db.ConfigDirectory;
import c5db.messages.generated.ControlMessages;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.MessageLite;
import org.jetlang.channels.Channel;
import org.jetlang.channels.RequestChannel;

import java.util.concurrent.ExecutionException;

import static c5db.messages.generated.ControlMessages.ModuleType;

/**
 * The root interface for all other modules and modules to get around inside the server.
 * <p/>
 * Provides bootstrapping and other module introspection and management utilities.  Ideally we can run multiple
 * OhmServer on the same JVM for testing (may be conflicts with the discovery methods).
 */
public interface OhmServer extends Service {
    /**
     * ** Interface type public methods *****
     */

    public long getNodeId();

    // TODO this could be generified if we used an interface instead of ModuleType
    public ListenableFuture<OhmModule> getModule(ModuleType moduleType);

    public Channel<MessageLite> getCommandChannel();

    public RequestChannel<MessageLite, ControlMessages.CommandReply> getCommandRequests();

    public Channel<ModuleStateChange> getModuleStateChangeChannel();

    public ImmutableMap<ModuleType, OhmModule> getModules() throws ExecutionException, InterruptedException;

    public ListenableFuture<ImmutableMap<ModuleType, OhmModule>> getModules2();

    public ConfigDirectory getConfigDirectory();

    public static class ModuleStateChange {
        public final OhmModule module;
        public final State state;

        @Override
        public String toString() {
            return "ModuleStateChange{" +
                    ", module=" + module +
                    ", state=" + state +
                    '}';
        }

        public ModuleStateChange(OhmModule module, State state) {
            this.module = module;
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
