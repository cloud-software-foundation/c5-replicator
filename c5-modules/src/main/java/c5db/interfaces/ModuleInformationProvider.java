/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.interfaces;

import c5db.messages.generated.ModuleType;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.Subscriber;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A service that provides information about C5Modules.
 */
public interface ModuleInformationProvider {

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
   * Each time a module comes online or goes offline, the returned Subscriber will emit
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
