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
import c5db.replication.rpc.RpcMessage;
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
              .appendText("an AppendEntries request"));
    }

    public static RequestMatcher aPreElectionPoll() {
      return new RequestMatcher().addCriterion(
          (request) -> request.getRequest().isPreElectionPollMessage(),
          (description) -> description.appendText("a PreElectionPoll"));
    }

    public static RequestMatcher aRequestVote() {
      return new RequestMatcher().addCriterion(
          (request) -> request.getRequest().isRequestVoteMessage(),
          (description) -> description.appendText("a RequestVote"));
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
              containsQuorumConfiguration(entryList(request), quorumConfig),
          (description) ->
              description.appendText(" with an entry containing the quorum configuration ")
                  .appendValue(quorumConfig));
    }

    private RequestMatcher addCriterion(Predicate<Request<RpcRequest, RpcWireReply>> predicate,
                                        Consumer<Description> describer) {
      RequestMatcher copy = new RequestMatcher();
      copy.predicates.addAll(this.predicates);
      copy.predicates.add(predicate);
      copy.describers.addAll(this.describers);
      copy.describers.add(describer);
      return copy;
    }
  }

  public static class ReplyMatcher extends TypeSafeMatcher<RpcMessage> {
    private final List<Predicate<RpcMessage>> predicates = new ArrayList<>();
    private final List<Consumer<Description>> describers = new ArrayList<>();

    @Override
    protected boolean matchesSafely(RpcMessage item) {
      return predicates.stream().allMatch((predicate) -> predicate.test(item));
    }

    @Override
    public void describeTo(Description description) {
      describers.forEach((describer) -> describer.accept(description));
    }

    public static ReplyMatcher anAppendReply() {
      return new ReplyMatcher().addCriterion(
          RpcMessage::isAppendReplyMessage,
          (description) -> description
              .appendText("an AppendEntries reply"));
    }

    public static ReplyMatcher aPreElectionReply() {
      return new ReplyMatcher().addCriterion(
          RpcMessage::isPreElectionReplyMessage,
          (description) -> description.appendText("a PreElectionReply")
      );
    }

    public ReplyMatcher withTerm(Matcher<Long> termMatcher) {
      return addCriterion(
          (reply) -> termMatcher.matches(reply.getAppendReplyMessage().getTerm()),
          (description) -> description.appendText(" with term ").appendDescriptionOf(termMatcher)
      );
    }

    public ReplyMatcher withResult(boolean success) {
      return addCriterion(
          (reply) ->
              reply.getAppendReplyMessage().getSuccess() == success,
          (description) ->
              description.appendText(" with result ").appendValue(success));
    }

    public ReplyMatcher withNextLogIndex(Matcher<Long> indexMatcher) {
      return addCriterion(
          (reply) ->
              indexMatcher.matches(reply.getAppendReplyMessage().getMyNextLogEntry()),
          (description) ->
              description.appendText(" with 'myNextLogEntry' ").appendDescriptionOf(indexMatcher));
    }

    public ReplyMatcher withPollResult(boolean wouldVote) {
      return addCriterion(
          (reply) ->
              reply.getPreElectionReplyMessage().getWouldVote() == wouldVote,
          (description) ->
              description.appendText(" with poll result ").appendValue(wouldVote));
    }

    private ReplyMatcher addCriterion(Predicate<RpcMessage> predicate,
                                      Consumer<Description> describer) {
      ReplyMatcher copy = new ReplyMatcher();
      copy.predicates.addAll(this.predicates);
      copy.predicates.add(predicate);
      copy.describers.addAll(this.describers);
      copy.describers.add(describer);
      return copy;
    }
  }

  public static boolean containsQuorumConfiguration(List<LogEntry> entryList, QuorumConfiguration configuration) {
    return entryList.stream().anyMatch(
        (entry) ->
            QuorumConfiguration.fromProtostuff(entry.getQuorumConfiguration())
                .equals(configuration));
  }

  private static boolean isAnAppendEntriesRequest(Request<RpcRequest, RpcWireReply> request) {
    return request.getRequest().getAppendMessage() != null;
  }

  private static List<LogEntry> entryList(Request<RpcRequest, RpcWireReply> request) {
    return request.getRequest().getAppendMessage().getEntriesList();
  }
}
