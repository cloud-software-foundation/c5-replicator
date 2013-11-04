/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static ohmdb.util.Graph.Node;
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
        A, B, C, D, E, F;
    }
}
