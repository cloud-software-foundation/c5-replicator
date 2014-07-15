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

import c5db.interfaces.discovery.NewNodeVisible;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.discovery.NodeInfoRequest;
import c5db.messages.generated.ModuleType;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.channels.RequestChannel;
import org.jetlang.channels.Subscriber;

/**
 * The discovery module is responsible for determining who the peers in a cluster are.  It
 * additionally provides the ability to translate node ids into network addresses (yes plural,
 * since machines sometimes have multiple network interfaces).
 * <p>
 */
@ModuleTypeBinding(ModuleType.Discovery)
public interface DiscoveryModule extends C5Module {
  RequestChannel<NodeInfoRequest, NodeInfoReply> getNodeInfo();

  ListenableFuture<NodeInfoReply> getNodeInfo(long nodeId, ModuleType module);

  ListenableFuture<ImmutableMap<Long, NodeInfo>> getState();

  Subscriber<NewNodeVisible> getNewNodeNotifications();
}
