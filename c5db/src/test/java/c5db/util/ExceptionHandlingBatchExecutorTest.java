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

import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExceptionHandlingBatchExecutorTest {

  private final MemoryChannel<Long> channel = new MemoryChannel<>();
  private final SettableFuture<Boolean> testFuture = SettableFuture.create();

  @Rule
  public ThrowFiberExceptions fiberExceptionHandler = new ThrowFiberExceptions();

  /**
   * One-off implementation of ThrowableRecipient which delegates to fiberExceptionHandler, then
   * uses the testFuture to notify the test thread that a throwable was actually caught; otherwise
   * the test might end before that happens.
   */
  private final Consumer<Throwable> throwableHandler = throwable -> {
    fiberExceptionHandler.accept(throwable);
    testFuture.set(true);
  };

  private final RunnableExecutor runnableExecutor = new RunnableExecutorImpl(
      new ExceptionHandlingBatchExecutor(throwableHandler));
  private Fiber fiber;

  /* Ordinarily one could check that a test method correctly throws an exception using built-in junit
   * capabilities. However, presently ThrowFiberExceptions, since it is itself a Rule, throws its
   * exceptions after junit's ExpectException comes into play. So another rule is required in order
   * to verify that the first Rule threw a given exception.
   */
  @Rule
  public TestRule ExpectException = (base, description) -> new Statement() {
    @Override
    public void evaluate() throws Throwable {
      try {
        base.evaluate();
        fail();
      } catch (IndexOutOfBoundsException ignored) {
        // expected exception; test succeeds
      }
    }
  };

  @Before
  public void before() {
    fiber = new ThreadFiber(runnableExecutor, null, true);
  }

  @After
  public void after() {
    fiber.dispose();
  }

  /**
   * The fiber is setup to rethrow any exceptions it encounters while executing. They will be
   * rethrown as part of the @Rule {@link ThrowFiberExceptions}, which throws after the @After
   * method. So this test will pass if it correctly generates an IndexOutOfBoundsException.
   *
   * @throws Exception
   */
  @Test
  public void testFiberCallbackException() throws Exception {
    channel.subscribe(fiber, (val) -> {
      throw new IndexOutOfBoundsException();
    });
    fiber.start();

    // The following line will lead to the IndexOutOfBoundsException above
    channel.publish(4L);

    // Ensure that the above callback is actually run before the test ends, by waiting for the future, which
    // will be set when the exception has been caught and handled.
    assertTrue(testFuture.get(10, TimeUnit.SECONDS));
  }
}
