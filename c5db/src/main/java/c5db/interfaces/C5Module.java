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
 * An internal module. Is a guava module.
 * <p/>
 * An internal module is a component that can be started/stopped, and is the official
 * internal interface between different modules.
 * <p/>
 * TODO module dependencies so if you stop one module, you have to stop the dependencies.
 */
public interface C5Module extends Service {

    public ModuleType getModuleType();

    public boolean hasPort();

    public int port();
}
