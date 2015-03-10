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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
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
  private final Map<String, EmptyCheckingQueue<Runnable>> keyQueues = new ConcurrentHashMap<>();

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

    flushAllQueues(timeout, unit);
    shutdownInternalExecutorService(timeout, unit);
  }

  /**
   * Retrieve the queue for the given key, creating it first if it does not exist
   */
  private EmptyCheckingQueue<Runnable> getQueueForKey(String key) {
    synchronized (keyQueues) {
      if (!keyQueues.containsKey(key)) {
        keyQueues.put(key, new EmptyCheckingQueue<Runnable>());
      }
      return keyQueues.get(key);
    }
  }

  /**
   * Wait for all tasks on all queues to complete
   */
  private void flushAllQueues(long timeout, TimeUnit unit) throws InterruptedException {
    synchronized (keyQueues) {
      final CountDownLatch submittedAllQueuedTasks = new CountDownLatch(keyQueues.size());

      for (EmptyCheckingQueue<Runnable> queue : keyQueues.values()) {
        enqueueOrRunTask(new Runnable() {
          @Override
          public void run() {
            submittedAllQueuedTasks.countDown();
          }
        }, queue);
      }
      submittedAllQueuedTasks.await(timeout, unit);
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
  private <T> Runnable createFutureSettingTaskRunner(final CheckedSupplier<T, Exception> task,
                                                     final SettableFuture<T> setWhenFinished) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          setWhenFinished.set(task.get());
        } catch (Throwable t) {
          LOG.error("Error executing task", t);
          setWhenFinished.setException(t);
        }
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
  private void submitToInternalExecutorService(final Runnable runnable, final EmptyCheckingQueue<Runnable> queue) {
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        runnable.run();
        Runnable nextTask = queue.discardHeadThenPeek();
        if (nextTask != null) {
          WrappingKeySerializingExecutor.this.submitToInternalExecutorService(nextTask, queue);
        }
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

