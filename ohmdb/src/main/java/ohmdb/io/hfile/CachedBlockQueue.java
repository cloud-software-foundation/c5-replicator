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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ohmdb.io.hfile;

import com.google.common.collect.MinMaxPriorityQueue;
import ohmdb.io.HeapSize;

/**
 * A memory-bound queue that will grow until an element brings
 * total size >= maxSize.  From then on, only entries that are sorted larger
 * than the smallest current entry will be inserted/replaced.
 *
 * <p>Use this when you want to find the largest elements (according to their
 * ordering, not their heap size) that consume as close to the specified
 * maxSize as possible.  Default behavior is to grow just above rather than
 * just below specified max.
 *
 * <p>Object used in this queue must implement {@link HeapSize} as well as
 * {@link Comparable}.
 */
public class CachedBlockQueue implements HeapSize {

  private MinMaxPriorityQueue<CachedBlock> queue;

  private long heapSize;
  private long maxSize;

  /**
   * @param maxSize the target size of elements in the queue
   * @param blockSize expected average size of blocks
   */
  public CachedBlockQueue(long maxSize, long blockSize) {
    int initialSize = (int)(maxSize / blockSize);
    if(initialSize == 0) initialSize++;
    queue = MinMaxPriorityQueue.expectedSize(initialSize).create();
    heapSize = 0;
    this.maxSize = maxSize;
  }

  /**
   * Attempt to add the specified cached block to this queue.
   *
   * <p>If the queue is smaller than the max size, or if the specified element
   * is ordered before the smallest element in the queue, the element will be
   * added to the queue.  Otherwise, there is no side effect of this call.
   * @param cb block to try to add to the queue
   */
  public void add(CachedBlock cb) {
    if(heapSize < maxSize) {
      queue.add(cb);
      heapSize += cb.heapSize();
    } else {
      CachedBlock head = queue.peek();
      if(cb.compareTo(head) > 0) {
        heapSize += cb.heapSize();
        heapSize -= head.heapSize();
        if(heapSize > maxSize) {
          queue.poll();
        } else {
          heapSize += head.heapSize();
        }
        queue.add(cb);
      }
    }
  }

  /**
   * @return The next element in this queue, or {@code null} if the queue is
   * empty.
   */
  public CachedBlock poll() {
    return queue.poll();
  }

  /**
   * @return The last element in this queue, or {@code null} if the queue is
   * empty.
   */
  public CachedBlock pollLast() {
    return queue.pollLast();
  }

  /**
   * Total size of all elements in this queue.
   * @return size of all elements currently in queue, in bytes
   */
  public long heapSize() {
    return heapSize;
  }
}
