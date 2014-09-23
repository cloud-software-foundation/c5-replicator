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

import c5db.messages.generated.ModuleType;

/**
 * Request information about a node and module. Results in a {@link c5db.interfaces.discovery.NodeInfoReply}
 */
public class NodeInfoRequest {
  public final long nodeId;
  public final ModuleType moduleType;

  public NodeInfoRequest(long nodeId, ModuleType moduleType) {
    this.nodeId = nodeId;
    this.moduleType = moduleType;
  }

  @Override
  public String toString() {
    return "NodeInfoRequest{" +
        "nodeId=" + nodeId +
        ", moduleType=" + moduleType +
        '}';
  }
}
