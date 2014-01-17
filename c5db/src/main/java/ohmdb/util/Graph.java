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


import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class Graph {
    public static <E extends Comparable<E>> List<ImmutableList<Node<E>>> doTarjan(Collection<Node<E>> allNodes0) {
        List<Node<E>> allNodes = new ArrayList<>(allNodes0);
        Collections.sort(allNodes);

        resetIndexes(allNodes);

        int index = 0;
        Deque<Node<E>> s = new LinkedList<>();
        List<ImmutableList<Node<E>>> components = new LinkedList<>();
        for (Node<E> v : allNodes) {

            index = strongConnect(index, s, v, components);
        }
        return components;
    }

    private static <E extends Comparable<E>> int strongConnect(int index,
                                                               Deque<Node<E>> s,
                                                               Node<E> v,
                                                               List<ImmutableList<Node<E>>> components) {
        if (v.index == -1) {
            v.index = index;
            v.lowlink = index;

            index += 1;

            s.push(v);

            for (Node<E> w : v.dependencies) {
                if (w.index == -1) {
                    index = strongConnect(index, s, w, components);

                    v.lowlink = Math.min(v.lowlink, w.lowlink);

                } else if (s.contains(w)) {
                    v.lowlink = Math.min(v.lowlink, w.index);
                }
            }

            if (v.lowlink == v.index) {
                List<Node<E>> component = new LinkedList<>();
                Node<E> w;
                do {
                    w = s.pop();
                    component.add(w);
                } while (w != v);
                //System.out.println("Component: " + component);
                components.add(ImmutableList.copyOf(component));
            }

        }
        return index;
    }

    private static <E extends Comparable<E>> void resetIndexes(Collection<Node<E>> values) {
        for (Node<E> n : values) {
            n.index = -1;
            n.lowlink = -1;
        }
    }

    public static class Node<E extends Comparable<E>> implements Comparable<Node<E>> {
        public final SortedSet<Node<E>> dependencies = new TreeSet<>();
        public final E type;
        public int index = -1;
        public int lowlink = -1;

        public Node(E type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "type=" + type +
                    ", dependencies=" + dependencies.size() +
                    ", index,lowlink=" + index + "," + lowlink +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node node = (Node) o;

            if (type != node.type) return false;

            return true;
        }

        @Override
        public int compareTo(Node<E> o) {
            return type.compareTo(o.type);
        }
    }
}
