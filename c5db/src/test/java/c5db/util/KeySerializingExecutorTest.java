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

import com.google.common.collect.Lists;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.States;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static c5db.CollectionMatchers.isNondecreasing;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class KeySerializingExecutorTest {
  private static final int NUM_TASKS = 20;
  private Mockery context = new JUnit4Mockery();

  @Test
  public void runsTasksSubmittedToIt() throws Exception {
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutor(sameThreadExecutor());
    CheckedSupplier task = context.mock(CheckedSupplier.class);

    context.checking(new Expectations() {{
      try {
        oneOf(task).get();
      } catch (Throwable ignore) {
      }
    }});

    //noinspection unchecked
    keySerializingExecutor.submit("key", task);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void submitsTasksOnceEachToTheSuppliedExecutorService() throws Exception {
    CheckedSupplier task = context.mock(CheckedSupplier.class);
    ExecutorService executorService = context.mock(ExecutorService.class);
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutor(executorService);

    final States submitted = context.states("submitted").startsAs("no");

    context.checking(new Expectations() {{
      try {
        // Allow one of the variants of submit or execute, exactly once.
        allowing(executorService).submit((Callable) anything());
        then(submitted.is("yes"));
        allowing(executorService).submit((Runnable) anything(), anything());
        then(submitted.is("yes"));
        allowing(executorService).submit((Runnable) anything());
        then(submitted.is("yes"));
        allowing(executorService).execute((Runnable) anything());
        then(submitted.is("yes"));

        never(executorService).submit((Callable) anything());
        when(submitted.is("yes"));
        never(executorService).submit((Runnable) anything(), anything());
        when(submitted.is("yes"));
        never(executorService).submit((Runnable) anything());
        when(submitted.is("yes"));
        never(executorService).execute((Runnable) anything());
        when(submitted.is("yes"));
      } catch (Throwable ignore) {
      }
    }});

    keySerializingExecutor.submit("key", task);
  }

  @Test(timeout = 1000)
  public void executesTasksAllHavingTheSameKeyInSeries() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutor(executorService);

    List<Integer> log =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key", NUM_TASKS);

    waitForTasksToFinish(keySerializingExecutor, "key");

    assertThat(log, hasSize(NUM_TASKS * 2));
    assertThat(log, isNondecreasing());
  }

  @Test(timeout = 1000)
  public void executesTasksForDifferentKeysEachSeparatelyInSeries() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    KeySerializingExecutor keySerializingExecutor = new KeySerializingExecutor(executorService);

    List<Integer> log1 =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key1", NUM_TASKS);
    List<Integer> log2 =
        submitSeveralTasksAndBeginLoggingTheirInvocations(keySerializingExecutor, "key2", NUM_TASKS);

    waitForTasksToFinish(keySerializingExecutor, "key1");
    waitForTasksToFinish(keySerializingExecutor, "key2");

    assertThat(log1, hasSize(NUM_TASKS * 2));
    assertThat(log1, isNondecreasing());
    assertThat(log2, hasSize(NUM_TASKS * 2));
    assertThat(log2, isNondecreasing());
  }

  private static List<Integer> submitSeveralTasksAndBeginLoggingTheirInvocations(
      KeySerializingExecutor keySerializingExecutor, String key, int howMany) throws Exception {

    List<Integer> log = Lists.newArrayList();
    for (int i = 0; i < howMany; i++) {
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
}
