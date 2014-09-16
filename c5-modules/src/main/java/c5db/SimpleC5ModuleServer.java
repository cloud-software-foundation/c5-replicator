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

package c5db;

import c5db.interfaces.C5Module;
import c5db.interfaces.ModuleServer;
import c5db.messages.generated.ModuleType;
import c5db.util.FiberOnly;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.fibers.Fiber;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * A basic C5Server; coordinates the interaction of the local modules
 */
public class SimpleC5ModuleServer implements ModuleServer {
  private final Fiber fiber;
  private final Consumer<Throwable> failureHandler;
  private final Map<ModuleType, C5Module> modules = new HashMap<>();
  private final Map<ModuleType, Integer> modulePorts = new HashMap<>();
  private final Channel<ImmutableMap<ModuleType, Integer>> modulePortsChannel = new MemoryChannel<>();

  public SimpleC5ModuleServer(Fiber fiber, Consumer<Throwable> failureHandler) {
    this.fiber = fiber;
    this.failureHandler = failureHandler;
  }

  public ListenableFuture<Service.State> startModule(C5Module module) {
    SettableFuture<Service.State> startedFuture = SettableFuture.create();
    Service.Listener stateChangeListener = new SimpleC5ModuleListener(
        module,
        () -> {
          addRunningModule(module);
          startedFuture.set(null);
        },
        () -> removeModule(module),
        failureHandler);

    module.addListener(stateChangeListener, fiber);
    modules.put(module.getModuleType(), module);
    module.start();

    return startedFuture;
  }

  @Override
  public ListenableFuture<C5Module> getModule(ModuleType moduleType) {
    SettableFuture<C5Module> moduleFuture = SettableFuture.create();
    fiber.execute(() -> moduleFuture.set(modules.get(moduleType)));
    return moduleFuture;
  }

  @Override
  public ListenableFuture<ImmutableMap<ModuleType, Integer>> getAvailableModulePorts() {
    final SettableFuture<ImmutableMap<ModuleType, Integer>> future = SettableFuture.create();
    fiber.execute(() -> future.set(ImmutableMap.copyOf(modulePorts)));
    return future;
  }

  @Override
  public Subscriber<ImmutableMap<ModuleType, Integer>> availableModulePortsChannel() {
    return modulePortsChannel;
  }

  @Override
  public ImmutableMap<ModuleType, C5Module> getModules()
      throws ExecutionException, InterruptedException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  @FiberOnly
  private void addRunningModule(C5Module module) {
    ModuleType type = module.getModuleType();
    if (modules.containsKey(type) && modules.get(type).equals(module)) {
      modulePorts.put(type, module.port());
      publishCurrentActivePorts();
    }
  }

  @FiberOnly
  private void removeModule(C5Module module) {
    ModuleType type = module.getModuleType();
    if (modules.containsKey(type) && modules.get(type).equals(module)) {
      modules.remove(type);
      modulePorts.remove(type);
      publishCurrentActivePorts();
    }
  }

  @FiberOnly
  private void publishCurrentActivePorts() {
    modulePortsChannel.publish(ImmutableMap.copyOf(modulePorts));
  }
}
