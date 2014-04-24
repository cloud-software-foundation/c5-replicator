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
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.States;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
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

public class KeySerializingExecutorDecoratorTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();
  private static final int NUM_TASKS = 20;
  private final ExecutorService fixedThreadExecutor = Executors.newFixedThreadPool(3);

  @SuppressWarnings("unchecked")
  private final CheckedSupplier<Integer, Exception> task = context.mock(CheckedSupplier.class);

  @Test
  public void runsTasksSubmittedToItAndReturnsTheirResult() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(sameThreadExecutor());

    context.checking(new Expectations() {{
      oneOf(task).get();
      will(returnValue(3));
    }});

    assertThat(keySerializingExecutor.submit("key", task), resultsIn(equalTo(3)));
  }

  @Test
  public void returnsFuturesSetWithTheExceptionsThrownBySubmittedTasks() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(sameThreadExecutor());

    context.checking(new Expectations() {{
      oneOf(task).get();
      will(throwException(new ArithmeticException()));
    }});

    assertThat(keySerializingExecutor.submit("key", task), resultsInException(ArithmeticException.class));
  }

  @Test
  public void submitsTasksOnceEachToTheSuppliedExecutorService() throws Exception {
    ExecutorService executorService = context.mock(ExecutorService.class);
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(executorService);

    context.checking(new Expectations() {{
      allowSubmitOrExecuteOnce(context, executorService);
    }});

    keySerializingExecutor.submit("key", task);
  }

  @Test(timeout = 1000)
  public void executesTasksAllHavingTheSameKeyInSeries() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(fixedThreadExecutor);

    List<Integer> log =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key");

    waitForTasksToFinish(keySerializingExecutor, "key");

    assertThat(log, containsRecordOfEveryTask());
    assertThat(log, isInTheOrderTheTasksWereSubmitted());
  }

  @Test(timeout = 1000)
  public void executesTasksForDifferentKeysEachSeparatelyInSeries() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(fixedThreadExecutor);

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
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(fixedThreadExecutor);
    keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS);
    keySerializingExecutor.submit("key", () -> null);
  }

  @Test
  public void onShutdownCompletesAllTasksThatHadBeenSubmittedPriorToShutdown() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutorDecorator(fixedThreadExecutor);

    List<Integer> log =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key");

    keySerializingExecutor.shutdownAndAwaitTermination(1, TimeUnit.SECONDS);

    assertThat(log, containsRecordOfEveryTask());
  }

  private static Matcher<Collection<?>> containsRecordOfEveryTask() {
    return hasSize(NUM_TASKS * 2);
  }

  private static <T extends Comparable<T>> Matcher<List<T>> isInTheOrderTheTasksWereSubmitted() {
    return CollectionMatchers.isNondecreasing();
  }

  private static List<Integer> submitSeveralTasksAndBeginLoggingTheirInvocations(
      KeySerializingExecutor keySerializingExecutor, String key) {

    List<Integer> log = new ArrayList<>();
    for (int i = 0; i < NUM_TASKS; i++) {
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

}
