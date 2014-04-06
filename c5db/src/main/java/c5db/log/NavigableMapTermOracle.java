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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * TermOracle using a NavigableMap, and not persisting any of its data.
 */
public class NavigableMapTermOracle implements TermOracle {
  private static final Logger LOG = LoggerFactory.getLogger(NavigableMapTermOracle.class);
  private final NavigableMap<Long, Long> map;

  public NavigableMapTermOracle(NavigableMap<Long, Long> initMap) {
    map = initMap;
  }

  public NavigableMapTermOracle() {
    this(new TreeMap<>());
  }

  @Override
  public void notifyLogging(long seqNum, long term) {
    final long lastTerm = getLastTerm();

    if (term > lastTerm) {
      map.put(seqNum, term);
    } else if (term < lastTerm) {
      LOG.error("Encountered a decreasing term, {}, where the last known term was {}", term, lastTerm);
      throw new IllegalArgumentException("Decreasing term number");
    }
  }

  @Override
  public void notifyTruncation(long seqNum) {
    map.tailMap(seqNum, true).clear();
  }

  @Override
  public long getTermAtSeqNum(long seqNum) {
    Map.Entry<Long, Long> entry = map.floorEntry(seqNum);
    return entry == null ? 0 : entry.getValue();
  }

  private long getLastTerm() {
    if (map.isEmpty()) {
      return 0;
    } else {
      return map.lastEntry().getValue();
    }
  }
}
