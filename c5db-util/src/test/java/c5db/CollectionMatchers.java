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

import java.util.Collection;
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
        description.appendText("a list whose elements are non-decreasing");
      }
    };
  }

  public static <T extends Comparable<T>> Matcher<List<T>> isStrictlyIncreasing() {
    return new TypeSafeMatcher<List<T>>() {
      @Override
      protected boolean matchesSafely(List<T> list) {
        for (int i = 1; i < list.size(); i++) {
          if (list.get(i).compareTo(list.get(i - 1)) <= 0) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a list whose elements are strictly increasing");
      }
    };
  }

  public static <T> Matcher<T> isIn(Collection<T> collection) {
    return new TypeSafeMatcher<T>() {
      @Override
      protected boolean matchesSafely(T item) {
        return collection.contains(item);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("an item contained within the collection ")
            .appendValue(collection);
      }
    };
  }
}
