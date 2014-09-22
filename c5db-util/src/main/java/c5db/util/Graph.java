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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Node node = (Node) o;

      return type == node.type;

    }

    @Override
    public int compareTo(Node<E> o) {
      return type.compareTo(o.type);
    }
  }
}
