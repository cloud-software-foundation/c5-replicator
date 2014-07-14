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

import c5db.generated.OLogHeader;
import c5db.interfaces.replication.QuorumConfiguration;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

/**
 * Matchers related to logs and log entries.
 */
public class LogMatchers {

  static Matcher<List<OLogEntry>> aListOfEntriesWithConsecutiveSeqNums(long start, long end) {
    return new TypeSafeMatcher<List<OLogEntry>>() {
      @Override
      protected boolean matchesSafely(List<OLogEntry> entries) {
        if (entries.size() != (end - start)) {
          return false;
        }
        long expectedSeqNum = start;
        for (OLogEntry entry : entries) {
          if (entry.getSeqNum() != expectedSeqNum) {
            return false;
          }
          expectedSeqNum++;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a list of OLogEntry with consecutive sequence numbers from ")
            .appendValue(start).appendText(" inclusive to ").appendValue(end).appendText(" exclusive");
      }
    };
  }

  static Matcher<OLogHeader> equalToHeader(OLogHeader header) {
    return new TypeSafeMatcher<OLogHeader>() {
      @Override
      protected boolean matchesSafely(OLogHeader item) {
        return item.getBaseSeqNum() == header.getBaseSeqNum()
            && item.getBaseTerm() == header.getBaseTerm()
            && QuorumConfiguration.fromProtostuff(item.getBaseConfiguration())
            .equals(QuorumConfiguration.fromProtostuff(header.getBaseConfiguration()));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" equal to ").appendValue(header);
      }
    };
  }
}
