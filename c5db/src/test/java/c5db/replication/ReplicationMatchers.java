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

package c5db.replication;

import c5db.RpcMatchers;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Assorted matchers used for replicator tests.
 */
class ReplicationMatchers {

  static Matcher<ReplicatorInstanceEvent> leaderElectedEvent(long minimumTerm) {
    return new TypeSafeMatcher<ReplicatorInstanceEvent>() {
      @Override
      protected boolean matchesSafely(ReplicatorInstanceEvent item) {
        return item.eventType == ReplicatorInstanceEvent.EventType.LEADER_ELECTED
            && item.leaderElectedTerm >= minimumTerm;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a ReplicatorInstanceEvent indicating a leader was elected");
      }
    };
  }

  static Matcher<IndexCommitNotice> aQuorumChangeCommitNotice(QuorumConfiguration quorumConfig) {
    return new TypeSafeMatcher<IndexCommitNotice>() {
      @Override
      protected boolean matchesSafely(IndexCommitNotice item) {
        return item.quorumConfig != null
            && item.quorumConfig.equals(quorumConfig);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a commit notice for quorum configuration ")
            .appendValue(quorumConfig);
      }
    };
  }

  static Matcher<IndexCommitNotice> aNoticeMatchingPeerAndCommitIndex(long peerId, long index) {
    return new TypeSafeMatcher<IndexCommitNotice>() {
      @Override
      protected boolean matchesSafely(IndexCommitNotice item) {
        return item.committedIndex >= index &&
            item.replicatorInstance.getId() == peerId;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a commit notice with index at least ")
            .appendValue(index)
            .appendText(" for peer ")
            .appendValue(peerId);
      }
    };
  }

  static Matcher<InRamTest.PeerController> theLeader() {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        return peer.isCurrentLeader();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("The peer who is the leader of the current term");
      }
    };
  }

  static Matcher<InRamTest.PeerController> hasCommittedEntriesUpTo(long index) {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        return peer.hasCommittedEntriesUpTo(index);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer has committed log entries up to index" + index);
      }
    };
  }

  static Matcher<InRamTest.PeerController> willCommitEntriesUpTo(long index) {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        try {
          peer.waitForCommit(index);
          assert peer.log.getLastIndex() >= index;
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will commit log entries up to index " + index);
      }

      @Override
      public void describeMismatchSafely(InRamTest.PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }

  static Matcher<InRamTest.PeerController> willRespondToAnAppendRequest() {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        try {
          peer.waitForAppendReply();
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will respond to an AppendEntries request");
      }

      @Override
      public void describeMismatchSafely(InRamTest.PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }

  static Matcher<InRamTest.PeerController> willSend(RpcMatchers.RequestMatcher requestMatcher) {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        try {
          peer.waitForRequest(requestMatcher);
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will send an AppendEntries request ").appendDescriptionOf(requestMatcher);
      }

      @Override
      public void describeMismatchSafely(InRamTest.PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }
}
