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
