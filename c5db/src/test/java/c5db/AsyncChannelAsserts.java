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

import c5db.util.ExceptionHandlingBatchExecutor;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.jetlang.channels.Channel;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.runners.model.MultipleFailureException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Helpers that allow us to assert or wait for channel messages from jetlang.
 *
 * TODO currently we create a fiber thread for every instance we run, maybe
 * consider using a fiber pool.
 */
public class AsyncChannelAsserts {

  public static class Listening<T> {
    final Fiber f;
    final ArrayBlockingQueue<T> messages;
    final List<Throwable> throwables;

    public Listening(Fiber f, ArrayBlockingQueue<T> messages,
                     List<Throwable> throwables) {
      this.f = f;
      this.messages = messages;
      this.throwables = throwables;
    }
  }

  public static <T> Listening<T> listenTo(Channel<T> channel) {
    List<Throwable> throwables = new ArrayList<>();
    BatchExecutor be = new ExceptionHandlingBatchExecutor(throwables::add);
    RunnableExecutor re = new RunnableExecutorImpl(be);
    Fiber f = new ThreadFiber(re, null, true);
    ArrayBlockingQueue<T> messages = new ArrayBlockingQueue<>(1);
    channel.subscribe(f, (m) -> {
      try {
        System.out.println("Message received: " + m);
        messages.put(m);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    f.start();
    return new Listening<>(f, messages, throwables);
  }

  public static <T> Matcher<T> publishesMessage(Matcher<T> m) {
    return m;
  }

  /**
   * Waits for a message that matches the matcher, if it doesn't happen within a reasonable
   * and short time frame, it will throw an assertion failure.
   * @param matcher the matcher which might match a message
   * @param <T> type
   * @throws Throwable
   */
  public static <T> void assertEventually(Listening<T> listener,
                                          Matcher<? super T> matcher) throws Throwable {
    helper(listener, matcher, true);
  }

  /**
   * Waits for a message that matches the matcher, if it doesn't happen within a reasonable
   * and short time frame, this method just returns.  No failures are thrown, the
   * assumption is mock expectations will illuminate the error.
   *
   * @param listener the listener you want
   * @param matcher the matcher which might match a message
   * @param <T> type
   * @throws Throwable
   */
  public static <T> void waitUntil(Listening<T> listener,
                                   Matcher<? super T> matcher) throws Throwable {
    helper(listener, matcher, false);
  }

  private static <T> void helper(Listening<T> listener,
                                 Matcher<? super T> matcher,
                                 boolean assertFail) throws Throwable {

    List<T> received = new ArrayList<>();
    while (true) {
      T msg = listener.messages.poll(5, TimeUnit.SECONDS);

      if (msg == null) {
        Description d = new StringDescription();
        matcher.describeTo(d);

        if (!received.isEmpty()) {
          d.appendText("we received messages:");
        }

        for (T m : received) {
          matcher.describeMismatch(m, d);
        }

        listener.f.dispose();

        if (assertFail) {
          listener.throwables.add(new AssertionError("Failing waiting for " + d.toString()));
          MultipleFailureException.assertEmpty(listener.throwables);
        }

        return;
      }

      if (matcher.matches(msg)) {
        listener.f.dispose();

        if (!listener.throwables.isEmpty()) {
          MultipleFailureException.assertEmpty(listener.throwables);
        }

        return;
      }

      received.add(msg);
    }
  }
}
