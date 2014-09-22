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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An executor service which accepts tasks (suppliers of some result) each with an associated
 * string key, and guarantees that all tasks associated with a given key will be run serially,
 * in the order they are submitted. No guarantee is made about the relative completion order
 * of tasks associated with different keys.
 * <p>
 * The notion of a well-defined "order in which tasks are submitted" requires that tasks with
 * the same key be submitted in a deterministic order, e.g., unaffected by thread scheduling.
 * <p/>
 * The purpose is so a single thread pool can handle IO requests for, potentially, several
 * different logs: the requests for each individual log need to be serialized with other
 * requests for that same log, and the "key" in that situation is some string that uniquely
 * identifies that log. However, in such a situation, IO requests for one log can be freely
 * interspersed with requests for another.
 */
public interface KeySerializingExecutor {
  /**
   * Submit a task for execution.
   *
   * @param key  Key associated with the task; the task will not be executed until all previously-submitted
   *             tasks with the same key have finished execution.
   * @param task A supplier of some result, which may throw an exception.
   * @param <T>  The type of the result produced by the task.
   * @return A future which will produce the result of the task, or else an exception.
   */
  <T> ListenableFuture<T> submit(String key, CheckedSupplier<T, Exception> task);

  /**
   * Shut down the executor. After calling this method, any call to submit will result in an exception. This
   * method will block until all previously submitted tasks complete, or until the specified time limit
   * expires, in which case a TimeoutException will be thrown.
   *
   * @param timeout Time to wait for previously submitted tasks to complete
   * @param unit    Time unit associated with timeout.
   */
  void shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;
}
