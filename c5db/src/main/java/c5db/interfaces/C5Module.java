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
  // TODO module dependencies so if you stop one module, you have to stop the dependencies.

  public ModuleType getModuleType();

  public boolean hasPort();

  public int port();
}
