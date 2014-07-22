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

package c5db.interfaces.log;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

import static c5db.interfaces.log.SequentialEntryIterable.SequentialEntryIterator;

public class SequentialEntryIterableMatchers {
  public static <E extends SequentialEntry> TypeSafeMatcher<SequentialEntryIterator<E>>
  isIteratorContainingInOrder(List<E> entries) {

    return new TypeSafeMatcher<SequentialEntryIterator<E>>() {
      private Exception exception;

      @Override
      protected boolean matchesSafely(SequentialEntryIterator<E> iterator) {
        int listIndex = 0;

        try {
          while (iterator.hasNext()) {
            E next = iterator.next();
            if (listIndex >= entries.size()
                || !next.equals(entries.get(listIndex))) {
              return false;
            }

            listIndex++;
          }
        } catch (Exception e) {
          exception = e;
          return false;
        }

        return listIndex == entries.size();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" a SequentialEntryIterator containing, in order, ")
            .appendValue(entries);
      }

      @Override
      protected void describeMismatchSafely(SequentialEntryIterator<E> item, Description mismatchDescription) {
        if (exception != null) {
          mismatchDescription.appendValue(exception);
        } else {
          super.describeMismatchSafely(item, mismatchDescription);
        }
      }
    };
  }
}
