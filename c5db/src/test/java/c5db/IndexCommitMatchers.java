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
package c5db;

import c5db.interfaces.ReplicationModule;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class IndexCommitMatchers {
  public static IndexCommitNoticeMatcher hasCommitNoticeIndexValueAtLeast(long indexValue) {
    return new IndexCommitNoticeMatcher(indexValue);
  }

  public static class IndexCommitNoticeMatcher extends TypeSafeMatcher<ReplicationModule.IndexCommitNotice> {
    private final long minimumIndexValue;

    public IndexCommitNoticeMatcher(long minimumIndexValue) {
      this.minimumIndexValue = minimumIndexValue;
    }

    @Override
    protected boolean matchesSafely(ReplicationModule.IndexCommitNotice item) {
      return item.committedIndex >= minimumIndexValue;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a index that is")
          .appendValue(minimumIndexValue);
    }
  }

}
