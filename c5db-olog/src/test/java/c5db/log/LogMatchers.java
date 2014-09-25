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

package c5db.log;

import c5db.interfaces.replication.QuorumConfiguration;
import c5db.log.generated.OLogHeader;
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
