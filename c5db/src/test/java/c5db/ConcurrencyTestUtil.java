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
                                                      ExceptionThrowingRunnable runnable) throws Exception {
    runNTimesAndWaitForAllToComplete(nTimes, executor, (int ignore) -> runnable.run());
  }

  private static ListenableFuture<Boolean> runAndReturnCompletionFuture(ExecutorService executor,
                                                                        IndexedExceptionThrowingRunnable runnable,
                                                                        int invocationIndex) {
    final SettableFuture<Boolean> setWhenFinished = SettableFuture.create();

    executor.execute(() -> {
      try {
        runnable.run(invocationIndex);
        setWhenFinished.set(true);
      } catch (Throwable t) {
        setWhenFinished.setException(t);
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
