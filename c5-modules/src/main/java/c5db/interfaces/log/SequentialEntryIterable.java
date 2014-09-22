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

package c5db.interfaces.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An Iterable-like-interface over SequentialEntry. Because the methods of Iterator cannot
 * throw checked exceptions, this interface does not extend Iterable; but, the semantics are
 * the same. The returned Iterator-like-interfaces are also Closeable, and must be closed
 * in order to release resources.
 *
 * @param <E> Type of entry over which to iterate
 */
public interface SequentialEntryIterable<E extends SequentialEntry> {

  SequentialEntryIterator<E> iterator() throws IOException;

  default void forEach(Consumer<? super E> action) throws IOException {
    Objects.requireNonNull(action);
    for (SequentialEntryIterator<E> iterator = this.iterator(); iterator.hasNext(); ) {
      action.accept(iterator.next());
    }
  }

  interface SequentialEntryIterator<E extends SequentialEntry> extends Closeable {
    boolean hasNext() throws IOException;

    E next() throws IOException;
  }
}
