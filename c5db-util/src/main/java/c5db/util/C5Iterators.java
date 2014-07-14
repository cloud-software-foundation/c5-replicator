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

import com.google.common.collect.Iterators;

import java.util.Iterator;

public class C5Iterators {

  /**
   * Lazily advance the passed Iterator the given number of times. A lazy variant of
   * {@link com.google.common.collect.Iterators#advance}
   *
   * @return A new Iterator object representing the given Iterator advanced a specified
   * number of times, calling the given Iterator's next() method when needed.
   */
  public static <T> Iterator<T> advanced(Iterator<T> backingIterator, int numberToAdvance) {
    return new Iterator<T>() {
      private boolean advanced;

      @Override
      public boolean hasNext() {
        lazilyAdvance();
        return backingIterator.hasNext();
      }

      @Override
      public T next() {
        lazilyAdvance();
        return backingIterator.next();
      }

      @Override
      public void remove() {
        lazilyAdvance();
        backingIterator.remove();
      }

      private void lazilyAdvance() {
        if (!advanced) {
          Iterators.advance(backingIterator, numberToAdvance);
          advanced = true;
        }
      }
    };
  }
}
