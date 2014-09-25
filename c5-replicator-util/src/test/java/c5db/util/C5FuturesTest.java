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
import org.hamcrest.Description;
import org.jetlang.fibers.Fiber;
import org.jmock.Expectations;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 */
public class C5FuturesTest {
  public static class RunARunnableAction implements Action {
    public RunARunnableAction() {
    }

    public Object invoke(Invocation invocation) throws Throwable {
      ((Runnable) invocation.getParameter(0)).run();
      return null;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("runs the runnable");
    }
  }

  public static Action runTheRunnable() {
    return new RunARunnableAction();
  }

  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery();
  Consumer<Integer> success = context.mock(Consumer.class, "success");
  Consumer<Throwable> failure = context.mock(Consumer.class, "failure");
  ListenableFuture<Integer> mockFuture = context.mock(ListenableFuture.class, "mockFuture");
  Fiber mockFiber = context.mock(Fiber.class);

  @Before
  public void setupRunnable() {
    // These expectations just indicate that we will run the runnable after addListener()
    // is called, no waiting around or anything.
    context.checking(new Expectations() {{
      oneOf(mockFuture).addListener(with(any(Runnable.class)), with(same(mockFiber)));
      will(runTheRunnable());
    }});
  }

  @Test
  public void testC5FutureCallback_Success() throws Exception {
    Integer futureValue = 12;

    context.checking(new Expectations() {{
      oneOf(mockFuture).get();
      will(returnValue(futureValue));

      oneOf(success).accept(futureValue);
    }});

    C5Futures.addCallback(mockFuture, success, failure, mockFiber);
  }

  @Test
  public void testC5FutureCallback_Failure() throws Exception {
    Exception error = new ExecutionException(new Exception());

    context.checking(new Expectations() {{
      oneOf(mockFuture).get();
      will(throwException(error));

      oneOf(failure).accept(error);
    }});

    C5Futures.addCallback(mockFuture, success, failure, mockFiber);
  }
}
