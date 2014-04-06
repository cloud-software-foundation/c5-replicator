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

import java.util.List;

public class CollectionMatchers {
  public static <T extends Comparable<T>> Matcher<List<T>> isNondecreasing() {
    return new TypeSafeMatcher<List<T>>() {
      @Override
      protected boolean matchesSafely(List<T> list) {
        for (int i = 1; i < list.size(); i++) {
          if (list.get(i).compareTo(list.get(i - 1)) < 0) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a list whose elements are nondecreasing");
      }
    };
  }
}
