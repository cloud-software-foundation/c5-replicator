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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FutureMatchers {
  private static final int TIMEOUT = 40000; // seconds

  public static <T> Matcher<Future<T>> resultsIn(Matcher<T> resultMatcher) {
    return returnsAFutureWhoseResult(resultMatcher);
  }

  public static <T> Matcher<Future<T>> returnsAFutureWhoseResult(Matcher<T> resultMatcher) {
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
}
