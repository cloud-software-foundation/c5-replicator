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