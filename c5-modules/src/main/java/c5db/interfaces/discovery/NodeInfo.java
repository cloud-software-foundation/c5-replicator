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

package c5db.interfaces.discovery;

import c5db.discovery.generated.Availability;
import c5db.discovery.generated.ModuleDescriptor;
import c5db.messages.generated.ModuleType;
import com.google.common.collect.ImmutableMap;

/**
 * Information about a node.
 */
public class NodeInfo {
  public final Availability availability;
  //public final Beacon.Availability availability;
  public final long lastContactTime;
  public final ImmutableMap<ModuleType, Integer> modules;

  public NodeInfo(Availability availability, long lastContactTime) {
    this.availability = availability;
    this.lastContactTime = lastContactTime;
    ImmutableMap.Builder<ModuleType, Integer> b = ImmutableMap.builder();
    if (availability.getModulesList() != null) {
      for (ModuleDescriptor moduleDescriptor : availability.getModulesList()) {
        b.put(moduleDescriptor.getModule(), moduleDescriptor.getModulePort());
      }
    }
    modules = b.build();
  }

  public NodeInfo(Availability availability) {
    this(availability, System.currentTimeMillis());
  }

  @Override
  public String toString() {
    return availability + " last contact: " + lastContactTime;
  }
}
