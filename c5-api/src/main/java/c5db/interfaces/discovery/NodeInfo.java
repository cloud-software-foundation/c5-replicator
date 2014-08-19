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
