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

package c5db.log;

import c5db.interfaces.log.Reader;
import c5db.util.CheckedSupplier;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

import static c5db.interfaces.log.SequentialEntryIterable.SequentialEntryIterator;

public class OLogReader implements Reader<OLogEntry> {
  @Override
  public ImmutableList<CheckedSupplier<SequentialEntryIterator<OLogEntry>, IOException>> getLogList()
      throws IOException {
    return null;
  }
}
