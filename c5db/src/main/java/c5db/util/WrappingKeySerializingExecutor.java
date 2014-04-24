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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An ExecutorService decorator which accepts tasks with an associated string key, and guarantees
 * that all tasks associated with a given key will be run serially, in the order they are
 * submitted, by the ExecutorService it decorates. This implementation is not safe for use by
 * multiple threads.
 */
public class WrappingKeySerializingExecutor implements KeySerializingExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(WrappingKeySerializingExecutor.class);
  private final ExecutorService executorService;
  private final Map<String, EmptyCheckingQueue<Runnable>> keyQueues = new HashMap<>();

  private boolean shutdown = false;

  public WrappingKeySerializingExecutor(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public <T> ListenableFuture<T> submit(String key, CheckedSupplier<T, Exception> task) {
    if (shutdown) {
      throw new RejectedExecutionException("WrappingKeySerializingExecutor already shut down");
    }
    addKeyIfItDoesNotExist(key);

    SettableFuture<T> finished = SettableFuture.create();
    Runnable taskRunner = createFutureSettingTaskRunner(task, finished);

    final EmptyCheckingQueue<Runnable> queue = keyQueues.get(key);
    if (queue.checkEmptyAndAdd(taskRunner)) {
      submitAndThenProcessNext(taskRunner, queue);
    }

    return finished;
  }

  @Override
  public void shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {

    final CountDownLatch submittedAllQueuedTasks = new CountDownLatch(keyQueues.size());
    for (String key : keyQueues.keySet()) {
      submit(key, () -> {
        submittedAllQueuedTasks.countDown();
        return null;
      });
    }

    shutdown = true;
    submittedAllQueuedTasks.await();
    shutdownInternalExecutorService(timeout, unit);
  }

  private void addKeyIfItDoesNotExist(String key) {
    if (!keyQueues.containsKey(key)) {
      keyQueues.put(key, new EmptyCheckingQueue<>());
    }
  }

  private <T> Runnable createFutureSettingTaskRunner(CheckedSupplier<T, Exception> task,
                                                     SettableFuture<T> setWhenFinished) {
    return () -> {
      try {
        setWhenFinished.set(task.get());
      } catch (Exception t) {
        LOG.error("Error executing WrappingKeySerializingExecutor task: {}", t);
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

  private void shutdownInternalExecutorService(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException {
    executorService.shutdown();
    boolean terminated = executorService.awaitTermination(timeout, unit);
    if (!terminated) {
      throw new TimeoutException("WrappingKeySerializingExecutor#shutdown");
    }
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

