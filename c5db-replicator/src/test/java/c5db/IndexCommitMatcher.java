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

import c5db.interfaces.replication.IndexCommitNotice;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class IndexCommitMatcher extends TypeSafeMatcher<IndexCommitNotice> {
  private final List<Predicate<IndexCommitNotice>> predicates = new ArrayList<>();
  private final List<Consumer<Description>> describers = new ArrayList<>();

  @Override
  protected boolean matchesSafely(IndexCommitNotice item) {
    return predicates.stream().allMatch((predicate) -> predicate.test(item));
  }

  @Override
  public void describeTo(Description description) {
    describers.forEach((describer) -> describer.accept(description));
  }

  public static IndexCommitMatcher aCommitNotice() {
    return new IndexCommitMatcher().addCriterion(
        (item) -> true,
        (description) -> description
            .appendText("an IndexCommitNotice"));
  }

  public IndexCommitMatcher withIndex(Matcher<Long> indexMatcher) {
    return addCriterion(
        (item) -> {
          // inefficient, but good enough for testing:
          for (long index = item.firstIndex; index <= item.lastIndex; index++) {
            if (indexMatcher.matches(index)) {
              return true;
            }
          }
          return false;
        },
        (description) ->
            description.appendText(" including log index ")
                .appendDescriptionOf(indexMatcher));
  }

  public IndexCommitMatcher withIndexRange(Matcher<Long> firstIndexMatcher, Matcher<Long> lastIndexMatcher) {
    return addCriterion(
        (item) ->
            firstIndexMatcher.matches(item.firstIndex)
                && lastIndexMatcher.matches(item.lastIndex),
        (description) ->
            description.appendText(" with first index ")
                .appendDescriptionOf(firstIndexMatcher)
                .appendText(" and last index ")
                .appendDescriptionOf(lastIndexMatcher));
  }

  public IndexCommitMatcher withTerm(Matcher<Long> termMatcher) {
    return addCriterion(
        (item) -> termMatcher.matches(item.term),
        (description) ->
            description.appendText(" with election term ").appendDescriptionOf(termMatcher));
  }

  public IndexCommitMatcher issuedFromPeer(long peerId) {
    return addCriterion(
        (item) -> item.nodeId == peerId,
        (description) ->
            description.appendText(" from peer ").appendValue(peerId));
  }

  private IndexCommitMatcher addCriterion(Predicate<IndexCommitNotice> predicate, Consumer<Description> describer) {
    IndexCommitMatcher copy = new IndexCommitMatcher();
    copy.predicates.addAll(this.predicates);
    copy.predicates.add(predicate);
    copy.describers.addAll(this.describers);
    copy.describers.add(describer);
    return copy;
  }
}
