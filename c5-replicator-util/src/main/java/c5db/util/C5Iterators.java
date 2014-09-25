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
