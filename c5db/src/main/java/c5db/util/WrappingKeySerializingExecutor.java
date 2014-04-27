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
 * An ExecutorService wrapper which accepts tasks with an associated string key, and guarantees
 * that all tasks associated with a given key will be run serially, in the order they are
 * submitted (if there is a definite order), by the wrapped ExecutorService.
 * <p>
 * This implementation is safe for use by multiple threads. However, if there are multiple task
 * submissions for the same key without a happens-before relationship, such as from multiple
 * unsynchronized threads, the implementation can make no guarantee about the order in which
 * those tasks are executed.
 */
public class WrappingKeySerializingExecutor implements KeySerializingExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(WrappingKeySerializingExecutor.class);
  private final ExecutorService executorService;
  private final Map<String, EmptyCheckingQueue<Runnable>> keyQueues = new HashMap<>();

  private volatile boolean shutdown = false;

  public WrappingKeySerializingExecutor(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public <T> ListenableFuture<T> submit(String key, CheckedSupplier<T, Exception> task) {
    if (shutdown) {
      throw new RejectedExecutionException("WrappingKeySerializingExecutor already shut down");
    }

    SettableFuture<T> taskFinishedFuture = SettableFuture.create();
    Runnable taskRunner = createFutureSettingTaskRunner(task, taskFinishedFuture);

    enqueueOrRunTask(taskRunner, getQueueForKey(key));

    return taskFinishedFuture;
  }

  @Override
  public void shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (getAndSetShutdown()) {
      return;
    }

    flushAllQueues();
    shutdownInternalExecutorService(timeout, unit);
  }

  /**
   * Retrieve the queue for the given key, creating it first if it does not exist
   */
  private EmptyCheckingQueue<Runnable> getQueueForKey(String key) {
    EmptyCheckingQueue<Runnable> queue = keyQueues.get(key);
    if (queue == null) {
      synchronized (keyQueues) {
        queue = keyQueues.get(key);
        if (queue == null) {
          keyQueues.put(key, queue = new EmptyCheckingQueue<>());
        }
      }
    }
    return queue;
  }

  /**
   * Wait for all tasks on all queues to complete
   */
  private void flushAllQueues() throws InterruptedException {
    synchronized (keyQueues) {
      final CountDownLatch submittedAllQueuedTasks = new CountDownLatch(keyQueues.size());

      for (EmptyCheckingQueue<Runnable> queue : keyQueues.values()) {
        enqueueOrRunTask(submittedAllQueuedTasks::countDown, queue);
      }
      submittedAllQueuedTasks.await();
    }
  }

  /**
   * Get and set shutdown as an atomic operation
   */
  private synchronized boolean getAndSetShutdown() {
    boolean prev = shutdown;
    shutdown = true;
    return prev;
  }

  /**
   * Create a Runnable that runs a task which produces a value, then sets the passed-in Future
   * with the produced value.
   */
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

  /**
   * Add a Runnable to the queue, and then run it if the queue was empty before adding the
   * Runnable. (If the queue was not empty, then the Runnable will be run as the queue is
   * consumed).
   */
  private void enqueueOrRunTask(Runnable runnable, EmptyCheckingQueue<Runnable> queue) {
    if (queue.checkEmptyAndAdd(runnable)) {
      submitToInternalExecutorService(runnable, queue);
    }
  }

  /**
   * Run the Runnable on the instance's ExecutorService. After it has run, if the queue
   * has any other tasks remaining, run the next one.
   */
  private void submitToInternalExecutorService(Runnable runnable, EmptyCheckingQueue<Runnable> queue) {
    executorService.submit(() -> {
      runnable.run();
      Runnable nextTask = queue.discardHeadThenPeek();
      if (nextTask != null) {
        submitToInternalExecutorService(nextTask, queue);
      }
    });
  }

  /**
   * Shut down the instance's ExecutorService and await its termination.
   */
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

