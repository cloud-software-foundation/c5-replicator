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

package c5db;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Utilities for running concurrency tests.
 */
public class ConcurrencyTestUtil {

  public static void runAConcurrencyTestSeveralTimes(int numThreads, int numAttempts, ConcurrencyTest test)
      throws Exception {
    final ExecutorService taskSubmitter = Executors.newFixedThreadPool(numThreads);

    for (int attempt = 0; attempt < numAttempts; attempt++) {
      test.run(numThreads * 2, taskSubmitter);
    }

    taskSubmitter.shutdown();
    taskSubmitter.awaitTermination(10, TimeUnit.SECONDS);
  }

  public static void runNTimesAndWaitForAllToComplete(int nTimes, ExecutorService executor,
                                                      IndexedExceptionThrowingRunnable runnable) throws Exception {
    final List<ListenableFuture<Boolean>> completionFutureList = new ArrayList<>(nTimes);

    for (int i = 0; i < nTimes; i++) {
      completionFutureList.add(
          runAndReturnCompletionFuture(executor, runnable, i));
    }

    waitForAll(completionFutureList);
  }

  public static void runNTimesAndWaitForAllToComplete(int nTimes, ExecutorService executor,
                                                      final ExceptionThrowingRunnable runnable) throws Exception {
    runNTimesAndWaitForAllToComplete(nTimes, executor, new IndexedExceptionThrowingRunnable() {
      @Override
      public void run(int ignore) throws Exception {
        runnable.run();
      }
    });
  }

  private static ListenableFuture<Boolean> runAndReturnCompletionFuture(ExecutorService executor,
                                                                        final IndexedExceptionThrowingRunnable runnable,
                                                                        final int invocationIndex) {
    final SettableFuture<Boolean> setWhenFinished = SettableFuture.create();

    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          runnable.run(invocationIndex);
          setWhenFinished.set(true);
        } catch (Throwable t) {
          setWhenFinished.setException(t);
        }
      }
    });
    return setWhenFinished;
  }

  private static void waitForAll(List<ListenableFuture<Boolean>> futures) throws Exception {
    Futures.allAsList(futures).get();
  }

  public interface ConcurrencyTest {
    void run(int degreeOfConcurrency, ExecutorService executorService) throws Exception;
  }

  public interface IndexedExceptionThrowingRunnable {
    void run(int indexIdentifyingThisInvocation) throws Exception;
  }

  public interface ExceptionThrowingRunnable {
    void run() throws Exception;
  }
}
