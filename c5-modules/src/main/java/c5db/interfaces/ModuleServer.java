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

import c5db.messages.generated.ModuleType;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Subscriber;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A server that organizes its work by delegating to various dynamically-assembled modules.
 */
public interface ModuleServer {

  // TODO this could be generified if we used an interface instead of ModuleType

  /**
   * This is primary mechanism via which modules with compile time binding via interfaces
   * that live in {@link c5db.interfaces} may obtain instances of their dependencies.
   * <p>
   * This method returns a future, which implies that the module may not be started yet.
   * The future will be signalled when the module is started, and callers may just add a
   * callback and wait.
   * <p>
   * In the future when automatic service startup order is working, this method might just
   * return the type without a future, or may not require much/any waiting.
   * <p>
   * Right now modules are specified via an enum, in the future perhaps we should
   * use a Java interface type?
   *
   * @param moduleType the specific module type you wish to retrieve
   * @return a future that will be set when the module is running
   */
  ListenableFuture<C5Module> getModule(ModuleType moduleType);

  /**
   * Get a future returning the map of currently online modules and their associated ports.
   */
  ListenableFuture<ImmutableMap<ModuleType, Integer>> getAvailableModulePorts();

  /**
   * Each time a module comes online or goes offline, the module server will emit
   * a complete map of the currently online modules, mapping them to their associated
   * ports.
   *
   * @return A Subscriber the caller may subscribe to to receive such maps.
   */
  Subscriber<ImmutableMap<ModuleType, Integer>> availableModulePortsChannel();

  /**
   * This is deprecated because it is synchronous and allows blocking implementations;
   * clients of this interface should use getModule instead, which is async.
   */
  @Deprecated
  ImmutableMap<ModuleType, C5Module> getModules() throws ExecutionException, InterruptedException, TimeoutException;
}
