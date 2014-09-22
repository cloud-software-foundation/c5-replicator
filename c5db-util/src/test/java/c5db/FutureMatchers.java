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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FutureMatchers {
  private static final int TIMEOUT = 4; // seconds

  public static <T> Matcher<Future<T>> resultsIn(Matcher<? super T> resultMatcher) {
    return returnsAFutureWhoseResult(resultMatcher);
  }

  public static <T> Matcher<Future<T>> resultsInException(Class<? extends Throwable> exceptionClass) {
    return returnsAFutureWithException(exceptionClass);
  }

  public static <T> Matcher<Future<T>> returnsAFutureWhoseResult(Matcher<? super T> resultMatcher) {
    return new TypeSafeMatcher<Future<T>>() {
      public Throwable throwable = null;

      @Override
      protected boolean matchesSafely(Future<T> item) {
        try {
          return resultMatcher.matches(item.get(TIMEOUT, TimeUnit.SECONDS));
        } catch (Exception t) {
          throwable = t;
          return false;
        }
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a Future whose result: ").appendDescriptionOf(resultMatcher);
      }

      @Override
      public void describeMismatchSafely(Future<T> item, Description description) {
        if (throwable != null) {
          description.appendValue(throwable);
        } else {
          description.appendText("got a Future with result: ");
          try {
            description.appendValue(item.get());
          } catch (Exception ignore) {
          }
        }
      }
    };
  }

  public static <T> Matcher<Future<T>> returnsAFutureWithException(Class<? extends Throwable> exceptionClass) {
    return new TypeSafeMatcher<Future<T>>() {
      public Throwable actualExceptionThrown = null;
      public T result;

      @Override
      protected boolean matchesSafely(Future<T> item) {
        try {
          result = item.get(TIMEOUT, TimeUnit.SECONDS);
          return false;
        } catch (Throwable t) {
          actualExceptionThrown = t.getCause();
          return exceptionClass.isInstance(actualExceptionThrown);
        }
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a Future resulting in: ").appendText(exceptionClass.getCanonicalName());
      }

      @Override
      public void describeMismatchSafely(Future<T> item, Description description) {
        description.appendText("got a Future with result: ");
        if (actualExceptionThrown != null) {
          description.appendValue(actualExceptionThrown);
        } else {
          description.appendValue(result);
        }
      }
    };
  }
}
