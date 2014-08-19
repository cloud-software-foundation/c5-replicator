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
