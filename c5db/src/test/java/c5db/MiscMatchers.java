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

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Matchers that don't belong in any other class, or generators or combinators of matchers.
 */
public class MiscMatchers {

  public static <T> Matcher<T> simpleMatcherForPredicate(Predicate<T> matches, Consumer<Description> describe) {
    return new TypeSafeMatcher<T>() {
      @Override
      protected boolean matchesSafely(T item) {
        return matches.test(item);
      }

      @Override
      public void describeTo(Description description) {
        describe.accept(description);
      }
    };
  }
}
