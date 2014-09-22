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
import com.google.common.util.concurrent.Service;

/**
 * A C5Module is the common interface for all modules in a C5 server.
 * A server consists of a number of {@link c5db.interfaces.C5Module}s running within
 * a JVM.  Each module has a lifecycle (eg: starting, started, failed, etc).  The only
 * way in which modules may interact with each other is via the public module interface
 * which must be inside the {@link c5db.interfaces} package.
 * <p>
 * To handle the module's lifecycle, it implements the guava {@link com.google.common.util.concurrent.Service}
 * interface.  Individual implementations would be encouraged to use guava's
 * {@link com.google.common.util.concurrent.AbstractService}.
 * <p>
 * Other features that we need to support in the future (that arent explicit here right now)
 * <ul>
 * <li>Module startup-dependencies (eg: module A needs module B to be running)</li>
 * <li>Module startup configuration (eg: what port should this module bind to?)</li>
 * </ul>
 * </p>
 */
public interface C5Module extends Service {
  ModuleType getModuleType();

  boolean hasPort();

  int port();

  String acceptCommand(String commandString) throws InterruptedException;
}
