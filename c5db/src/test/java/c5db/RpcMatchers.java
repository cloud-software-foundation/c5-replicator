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

import c5db.replication.QuorumConfiguration;
import c5db.replication.generated.LogEntry;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetlang.channels.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Matchers for RPC requests and/or replies.
 */
public class RpcMatchers {

  public static class RequestMatcher extends TypeSafeMatcher<Request<RpcRequest, RpcWireReply>> {
    private final List<Predicate<Request<RpcRequest, RpcWireReply>>> predicates = new ArrayList<>();
    private final List<Consumer<Description>> describers = new ArrayList<>();

    @Override
    protected boolean matchesSafely(Request<RpcRequest, RpcWireReply> item) {
      return predicates.stream().allMatch((predicate) -> predicate.test(item));
    }

    @Override
    public void describeTo(Description description) {
      describers.forEach((describer) -> describer.accept(description));
    }

    public static RequestMatcher anAppendRequest() {
      return new RequestMatcher().addCriterion(
          RpcMatchers::isAnAppendEntriesRequest,
          (description) -> description
              .appendText("an AppendEntries request "));
    }

    public RequestMatcher from(long peerId) {
      return addCriterion(
          (request) ->
              request.getRequest().from == peerId,
          (description) ->
              description
                  .appendText(" from ").appendValue(peerId));
    }

    public RequestMatcher to(long peerId) {
      return addCriterion(
          (request) ->
              request.getRequest().to == peerId,
          (description) ->
              description
                  .appendText(" to ").appendValue(peerId));
    }

    public RequestMatcher containingEntryIndex(long index) {
      return addCriterion(
          (request) ->
              entryList(request).stream().anyMatch((entry) -> entry.getIndex() == index),
          (description) ->
              description.appendText(" containing a log entry with index ").appendValue(index));
    }

    public RequestMatcher withCommitIndex(Matcher<Long> indexMatcher) {
      return addCriterion(
          (request) ->
              indexMatcher.matches(request.getRequest().getAppendMessage().getCommitIndex()),
          (description) ->
              description.appendText(" with commitIndex ").appendDescriptionOf(indexMatcher));
    }

    public RequestMatcher withPrevLogIndex(Matcher<Long> indexMatcher) {
      return addCriterion(
          (request) ->
              indexMatcher.matches(request.getRequest().getAppendMessage().getPrevLogIndex()),
          (description) ->
              description.appendText(" with prevLogIndex ").appendDescriptionOf(indexMatcher));
    }

    public RequestMatcher containingQuorumConfig(QuorumConfiguration quorumConfig) {
      return addCriterion(
          (request) ->
              entryList(request).stream().anyMatch(
                  (entry) ->
                      QuorumConfiguration.fromProtostuff(entry.getQuorumConfiguration())
                          .equals(quorumConfig)),
          (description) ->
              description.appendText(" with an entry containing the quorum configuration ")
                  .appendValue(quorumConfig));
    }

    private RequestMatcher addCriterion(Predicate<Request<RpcRequest, RpcWireReply>> predicate,
                                        Consumer<Description> describer) {
      predicates.add(predicate);
      describers.add(describer);
      return this;
    }
  }

  private static boolean isAnAppendEntriesRequest(Request<RpcRequest, RpcWireReply> request) {
    return request.getRequest().getAppendMessage() != null;
  }

  private static List<LogEntry> entryList(Request<RpcRequest, RpcWireReply> request) {
    return request.getRequest().getAppendMessage().getEntriesList();
  }
}
