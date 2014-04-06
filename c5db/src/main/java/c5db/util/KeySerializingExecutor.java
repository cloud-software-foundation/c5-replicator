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

package c5db.util;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An ExecutorService decorator which accepts tasks with an associated string key, and guarantees
 * that all tasks associated with a given key will be run serially, in the order they are
 * submitted. No guarantee is made about tasks associated with different keys.
 * <p>
 * The purpose is so a single thread pool can handle IO requests for, potentially, several
 * different logs: the requests for each individual log need to be serialized with other
 * requests for that same log, and the "key" in that situation is some string that uniquely
 * identifies that log. However, in such a situation, IO requests for one log can be freely
 * interspersed with requests for another.
 */
public class KeySerializingExecutor {
  private final ExecutorService executorService;
  private final Map<String, EmptyCheckingQueue<Runnable>> keyQueues = new HashMap<>();

  public KeySerializingExecutor(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public <T> ListenableFuture<T> submit(String key, CheckedSupplier<T, Exception> task) {
    if (!keyQueues.containsKey(key)) {
      keyQueues.put(key, new EmptyCheckingQueue<>());
    }

    SettableFuture<T> finished = SettableFuture.create();
    Runnable taskRunner = createTaskRunner(task, finished);

    final EmptyCheckingQueue<Runnable> queue = keyQueues.get(key);
    if (queue.checkEmptyAndAdd(taskRunner)) {
      submitAndThenProcessNext(taskRunner, queue);
    }

    return finished;
  }

  public void shutdown() {
    executorService.shutdown();
    keyQueues.clear();
  }

  private <T> Runnable createTaskRunner(CheckedSupplier<T, Exception> task,
                                        SettableFuture<T> setWhenFinished) {
    return () -> {
      try {
        setWhenFinished.set(task.get());
      } catch (Exception t) {
        setWhenFinished.setException(t);
      }
    };
  }

  private void submitAndThenProcessNext(Runnable runnable, EmptyCheckingQueue<Runnable> queue) {
    executorService.submit(() -> {
      runnable.run();
      Runnable nextTask = queue.discardHeadThenPeek();
      if (nextTask != null) {
        submitAndThenProcessNext(nextTask, queue);
      }
    });
  }

  private class EmptyCheckingQueue<Q> {
    private final Queue<Q> queue = new LinkedList<>();
    private final Lock lock = new ReentrantLock();

    public boolean checkEmptyAndAdd(Q item) {
      lock.lock();
      try {
        boolean empty = queue.isEmpty();
        queue.add(item);
        return empty;
      } finally {
        lock.unlock();
      }
    }

    public Q discardHeadThenPeek() {
      lock.lock();
      try {
        queue.poll();
        return queue.peek();
      } finally {
        lock.unlock();
      }
    }
  }
}

