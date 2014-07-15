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
