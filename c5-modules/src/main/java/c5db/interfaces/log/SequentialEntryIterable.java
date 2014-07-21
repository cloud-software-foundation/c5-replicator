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
