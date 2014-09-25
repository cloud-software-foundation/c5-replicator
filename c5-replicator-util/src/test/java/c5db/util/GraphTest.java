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

package c5db.util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static c5db.util.Graph.Node;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 */
public class GraphTest {
  @Test
  public void testDoTarjan() throws Exception {
    Map<NodeType, Node<NodeType>> nodes = new HashMap<>();
    // create a node for each NodeType:
    for (NodeType t : NodeType.values()) {
      nodes.put(t, new Node<>(t));
    }

    // attach nodes like so:
    // A -> B
    // B -> C
    // C -> D
    // C -> E
    // E -> F
    // D -> F
    connectFromTo(nodes, NodeType.A, NodeType.B);
    connectFromTo(nodes, NodeType.B, NodeType.C);
    connectFromTo(nodes, NodeType.C, NodeType.D);
    connectFromTo(nodes, NodeType.C, NodeType.E);
    connectFromTo(nodes, NodeType.E, NodeType.F);
    connectFromTo(nodes, NodeType.D, NodeType.F);

    List<ImmutableList<Node<NodeType>>> result = Graph.doTarjan(nodes.values());
    System.out.println("no cycles");
    Joiner joiner = Joiner.on("\n");
    System.out.println(joiner.join(result));
    assertEquals(nodes.size(), result.size());
    // validate topo sort:
    validateTopoSort(ImmutableList.of(NodeType.A, NodeType.B, NodeType.C,
        NodeType.E, NodeType.D, NodeType.F), result);

    // create a cycle:
    connectFromTo(nodes, NodeType.F, NodeType.C);
    result = Graph.doTarjan(nodes.values());
    System.out.println("cycles");
    System.out.println(joiner.join(result));
    assertFalse(nodes.size() == result.size());
  }

  private void validateTopoSort(ImmutableList<NodeType> expected, List<ImmutableList<Node<NodeType>>> result) {
    // reverse the expected order:
    ImmutableList<NodeType> reverse = expected.reverse();
    Deque<ImmutableList<Node<NodeType>>> q = new LinkedList<>(result);
    for (NodeType t : reverse) {
      ImmutableList<Node<NodeType>> topoNode = q.pop();
      assertEquals(1, topoNode.size());
      assertEquals(t, topoNode.get(0).type);
    }
  }

  private void connectFromTo(Map<NodeType, Node<NodeType>> nodes, NodeType a, NodeType b) {
    nodes.get(a).dependencies.add(nodes.get(b));
  }

  private static enum NodeType {
    A, B, C, D, E, F
  }
}
