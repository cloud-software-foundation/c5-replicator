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

import java.util.List;

/**
 * Information about a node/module in response to a request.
 */
public class NodeInfoReply {
  /**
   * Was the node/module information found?
   */
  public final boolean found;
  public final List<String> addresses;
  public final int port;

  public NodeInfoReply(boolean found, List<String> addresses, int port) {
    this.found = found;
    this.addresses = addresses;
    this.port = port;
  }

  @Override
  public String toString() {
    return "NodeInfoReply{" +
        "found=" + found +
        ", addresses=" + addresses +
        ", port=" + port +
        '}';
  }

  public final static NodeInfoReply NO_REPLY = new NodeInfoReply(false, null, 0);
}
