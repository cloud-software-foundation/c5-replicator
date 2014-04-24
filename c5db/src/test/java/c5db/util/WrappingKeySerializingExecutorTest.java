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

import c5db.CollectionMatchers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.States;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static c5db.FutureMatchers.resultsIn;
import static c5db.FutureMatchers.resultsInException;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

public class WrappingKeySerializingExecutorTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();
  private final ExecutorService fixedThreadExecutor = Executors.newFixedThreadPool(3);
  private final ExecutorService executorService = context.mock(ExecutorService.class);

  private static int numTasks = 20;

  @SuppressWarnings("unchecked")
  private final CheckedSupplier<Integer, Exception> task = context.mock(CheckedSupplier.class);

  @Test
  public void runsTasksSubmittedToItAndReturnsTheirResult() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(sameThreadExecutor());

    context.checking(new Expectations() {{
      oneOf(task).get();
      will(returnValue(3));
    }});

    assertThat(keySerializingExecutor.submit("key", task), resultsIn(equalTo(3)));
  }

  @Test
  public void returnsFuturesSetWithTheExceptionsThrownBySubmittedTasks() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(sameThreadExecutor());

    context.checking(new Expectations() {{
      oneOf(task).get();
      will(throwException(new ArithmeticException("Expected as part of test")));
    }});

    assertThat(keySerializingExecutor.submit("key", task), resultsInException(ArithmeticException.class));
  }

  @Test
  public void submitsTasksOnceEachToTheSuppliedExecutorService() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(executorService);

    context.checking(new Expectations() {{
      allowSubmitOrExecuteOnce(context, executorService);
    }});

    keySerializingExecutor.submit("key", task);
  }

  @Test(timeout = 1000)
  public void executesTasksAllHavingTheSameKeyInSeries() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(fixedThreadExecutor);

    List<Integer> log =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key");

    waitForTasksToFinish(keySerializingExecutor, "key");

    assertThat(log, containsRecordOfEveryTask());
    assertThat(log, isInTheOrderTheTasksWereSubmitted());
  }

  @Test(timeout = 1000)
  public void executesTasksForDifferentKeysEachSeparatelyInSeries() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(fixedThreadExecutor);

    List<Integer> log1 =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key1");
    List<Integer> log2 =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key2");

    waitForTasksToFinish(keySerializingExecutor, "key1");
    waitForTasksToFinish(keySerializingExecutor, "key2");

    assertThat(log1, containsRecordOfEveryTask());
    assertThat(log1, isInTheOrderTheTasksWereSubmitted());
    assertThat(log2, containsRecordOfEveryTask());
    assertThat(log2, isInTheOrderTheTasksWereSubmitted());
  }

  @Test(expected = RejectedExecutionException.class)
  public void throwsAnExceptionIfATaskIsSubmittedAfterShutdownIsCalled() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(fixedThreadExecutor);
    keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS);
    keySerializingExecutor.submit("key", () -> null);
  }

  @Test
  public void onShutdownCompletesAllTasksThatHadBeenSubmittedPriorToShutdown() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(fixedThreadExecutor);

    List<Integer> log =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key");

    keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS);

    assertThat(log, containsRecordOfEveryTask());
  }

  @Test(timeout = 3000)
  public void acceptsSubmissionsFromMultipleThreadsConcurrentlyWithEachThreadADifferentKey() throws Exception {
    final int numThreads = 20;
    final int numAttempts = 150;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, this::executeAMultikeySubmissionConcurrencyStressTest);
  }

  @Test(timeout = 3000)
  public void acceptsSubmissionsFromMultipleThreadsConcurrentlyWithinOneKeyWithExecutionOrderUndetermined()
      throws Exception {
    final int numThreads = 50;
    final int numAttempts = 300;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, this::executeASingleKeyConcurrencyStressTest);
  }

  @Test(timeout = 3000)
  public void shutsDownIdempotently() throws Exception {
    final int numThreads = 10;
    final int numAttempts = 300;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, this::executeAShutdownIdempotencyStressTest);
  }

  @Test(timeout = 3000)
  public void shutsDownAtomicallyWithRespectToSubmitAttempts() throws Exception {
    final int numThreads = 5;
    final int numAttempts = 100;

    runAConcurrencyTestSeveralTimes(numThreads, numAttempts, this::executeAShutdownAtomicityStressTest);
  }


  private void runAConcurrencyTestSeveralTimes(int numThreads, int numAttempts, ConcurrencyTest test)
      throws Exception {
    final ExecutorService taskSubmitter = Executors.newFixedThreadPool(numThreads);

    for (int attempt = 0; attempt < numAttempts; attempt++) {
      test.run(numThreads * 2, taskSubmitter);
    }

    taskSubmitter.shutdown();
    taskSubmitter.awaitTermination(3, TimeUnit.SECONDS);

  }

  private static List<Integer> submitSeveralTasksAndBeginLoggingTheirInvocations(
      KeySerializingExecutor keySerializingExecutor, String key) {

    List<Integer> log = new ArrayList<>(numTasks * 2);
    for (int i = 0; i < numTasks; i++) {
      keySerializingExecutor.submit(key, getSupplierWhichLogsItsNumberTwice(i, log));
    }
    return log;
  }

  private static CheckedSupplier<Integer, Exception> getSupplierWhichLogsItsNumberTwice(
      int instanceNumber, List<Integer> log) {

    return () -> {
      log.add(instanceNumber);
      Thread.yield();
      log.add(instanceNumber);
      return 0;
    };
  }

  private static void waitForTasksToFinish(KeySerializingExecutor keySerializingExecutor, String key)
      throws Exception {
    keySerializingExecutor.submit(key, () -> 0).get();
  }

  public static void allowSubmitOrExecuteOnce(Mockery context, ExecutorService executorService) {
    final States submitted = context.states("submitted").startsAs("no");

    context.checking(new Expectations() {{
      allowSubmitAndThen(context, executorService, submitted.is("yes"));
      doNowAllowSubmitOnce(context, executorService, submitted.is("yes"));
    }});
  }

  @SuppressWarnings("unchecked")
  private static void allowSubmitAndThen(Mockery context,
                                         ExecutorService executorService,
                                         org.jmock.internal.State state) {
    context.checking(new Expectations() {{
      allowing(executorService).submit(with.<Callable>is(any(Callable.class)));
      then(state);
      allowing(executorService).submit(with.is(any(Runnable.class)), with.is(any(Object.class)));
      then(state);
      allowing(executorService).submit(with.<Runnable>is(any(Runnable.class)));
      then(state);
      allowing(executorService).execute(with.is(any(Runnable.class)));
      then(state);
    }});
  }

  @SuppressWarnings("unchecked")
  private static void doNowAllowSubmitOnce(Mockery context,
                                           ExecutorService executorService,
                                           org.jmock.internal.State state) {
    context.checking(new Expectations() {{
      never(executorService).submit(with.<Callable>is(any(Callable.class)));
      when(state);
      never(executorService).submit(with.is(any(Runnable.class)), with.is(any(Object.class)));
      when(state);
      never(executorService).submit(with.<Runnable>is(any(Runnable.class)));
      when(state);
      never(executorService).execute(with.is(any(Runnable.class)));
      when(state);
    }});
  }

  private void executeAMultikeySubmissionConcurrencyStressTest(int numberOfSubmissions, ExecutorService taskSubmitter)
      throws Exception {
    final KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(
        Executors.newSingleThreadExecutor());

    runSeveralSimultaneousSeriesOfTasksAndWaitForAllToComplete(
        numberOfSubmissions, taskSubmitter, keySerializingExecutor);

    keySerializingExecutor.shutdownAndAwaitTermination(2, TimeUnit.SECONDS);
  }

  private void runSeveralSimultaneousSeriesOfTasksAndWaitForAllToComplete(
      int numSimultaneous,
      ExecutorService executorThatSubmitsTasks,
      KeySerializingExecutor executorThatRunsTasks) throws Exception {
    final List<ListenableFuture<Boolean>> completedFutureList = new ArrayList<>(numSimultaneous);

    for (int i = 0; i < numSimultaneous; i++) {
      final String key = keyNumber(i);

      completedFutureList.add(
          runAndReturnCompletionFuture(executorThatSubmitsTasks,
              () -> runSeriesOfTasksForOneKey(executorThatRunsTasks, key)));
    }

    waitForAll(completedFutureList);
  }

  private String keyNumber(int i) {
    return "key" + String.valueOf(i);
  }

  private void runSeriesOfTasksForOneKey(KeySerializingExecutor keySerializingExecutor,
                                         String key) throws Exception {
    setNumberOfTasks(2);

    List<Integer> log = submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, key);
    waitForTasksToFinish(keySerializingExecutor, key);
    assertThat(log, containsRecordOfEveryTask());
    assertThat(log, isInTheOrderTheTasksWereSubmitted());
  }

  private static void setNumberOfTasks(int n) {
    numTasks = n;
  }

  private void executeASingleKeyConcurrencyStressTest(int numCalls, ExecutorService executor)
      throws Exception {
    final KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(
        Executors.newSingleThreadExecutor());
    final List<Integer> taskResults = Collections.synchronizedList(new ArrayList<>(numCalls));

    // The keySerializingExecutor can make no guarantee about the order in which tasks will
    // be completed if added with the same key from multiple threads; only that they will all be completed.
    runNTimesAndWaitForAllToComplete(numCalls, executor,
        () -> {
          keySerializingExecutor.submit("key", () -> {
            taskResults.add(0);
            return 0;
          });
        });

    keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS);
    assertThat(taskResults, hasSize(numCalls));
  }

  private void executeAShutdownIdempotencyStressTest(int numShutdownCalls,
                                                     ExecutorService shutdownCallingService) throws Exception {
    final KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(
        Executors.newSingleThreadExecutor());
    keySerializingExecutor.submit("key", () -> null).get();

    runNTimesAndWaitForAllToComplete(numShutdownCalls, shutdownCallingService,
        () -> keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS));
  }

  private void executeAShutdownAtomicityStressTest(int numberOfSubmissions,
                                                   ExecutorService executor) throws Exception {
    final KeySerializingExecutor keySerializingExecutor = new WrappingKeySerializingExecutor(
        Executors.newSingleThreadExecutor());

    final List<ListenableFuture<Boolean>> completedFutureList = new ArrayList<>();

    // Simply call shutdown interspersed with other submit calls and ensure there are no errors
    // However, it is nondeterministic which, if any, of the submits will go through.
    for (int i = 0; i < numberOfSubmissions; i++) {
      final String key = keyNumber(i);

      completedFutureList.add(
          runAndReturnCompletionFuture(executor,
              () -> {
                try {
                  keySerializingExecutor.submit(key, () -> null);
                } catch (RejectedExecutionException ignore) {
                }
              }));

      if (i == numberOfSubmissions / 2) {
        completedFutureList.add(
            runAndReturnCompletionFuture(executor,
                () -> keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS)));
      }
    }

    waitForAll(completedFutureList);
  }


  private void runNTimesAndWaitForAllToComplete(int nTimes, ExecutorService executor,
                                                ExceptionThrowingRunnable runnable)
      throws Exception {
    final List<ListenableFuture<Boolean>> completedFutureList = new ArrayList<>(nTimes);

    for (int i = 0; i < nTimes; i++) {
      completedFutureList.add(
          runAndReturnCompletionFuture(executor, runnable));
    }

    waitForAll(completedFutureList);
  }

  private ListenableFuture<Boolean> runAndReturnCompletionFuture(ExecutorService executor,
                                                                 ExceptionThrowingRunnable runnable) {
    final SettableFuture<Boolean> setWhenFinished = SettableFuture.create();

    executor.execute(() -> {
      try {
        runnable.run();
        setWhenFinished.set(true);
      } catch (Throwable t) {
        setWhenFinished.setException(t);
      }
    });
    return setWhenFinished;
  }

  private interface ExceptionThrowingRunnable {
    void run() throws Exception;
  }

  private interface ConcurrencyTest {
    void run(int degreeOfConcurrency, ExecutorService executorService) throws Exception;
  }

  private void waitForAll(List<ListenableFuture<Boolean>> futures) throws Exception {
    Futures.allAsList(futures).get();
  }

  private static Matcher<Collection<?>> containsRecordOfEveryTask() {
    return hasSize(numTasks * 2);
  }

  private static <T extends Comparable<T>> Matcher<List<T>> isInTheOrderTheTasksWereSubmitted() {
    return CollectionMatchers.isNondecreasing();
  }
}
