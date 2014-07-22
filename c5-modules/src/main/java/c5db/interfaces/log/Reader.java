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

import c5db.util.CheckedSupplier;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

import static c5db.interfaces.log.SequentialEntryIterable.SequentialEntryIterator;

/**
 * A reader of persisted logs. A single quorum may have several persistence objects (e.g.
 * files) across which its continuity of logs are stored. This interface allows access to
 * each of those logs for a given quorum; and for each one, it allows to iterate over the
 * entries contained in the log.
 *
 * @param <E> The type of entry of the logs.
 */
public interface Reader<E extends SequentialEntry> {

  /**
   * Get a list of the logs served by this Reader. Each one is represented by a supplier of
   * SequentialEntryIterator; the supplied iterators must be closed by the user of this
   * interface.
   *
   * @return An immutable list of the available logs.
   * @throws IOException
   */
  ImmutableList<CheckedSupplier<SequentialEntryIterator<E>, IOException>> getLogList() throws IOException;
}