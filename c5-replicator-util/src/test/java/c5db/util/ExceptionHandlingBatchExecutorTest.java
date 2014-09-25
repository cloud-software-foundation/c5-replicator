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
  public JUnitRuleFiberExceptions fiberExceptionHandler = new JUnitRuleFiberExceptions();

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
   * rethrown as part of the @Rule {@link JUnitRuleFiberExceptions}, which throws after the @After
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
