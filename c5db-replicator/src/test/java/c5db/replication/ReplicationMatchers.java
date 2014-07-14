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
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.replication.generated.LogEntry;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Assorted matchers used for replicator tests.
 */
class ReplicationMatchers {

  static Matcher<ReplicatorInstanceEvent> leaderElectedEvent(Matcher<Long> leaderMatcher,
                                                             Matcher<Long> termMatcher) {
    return new TypeSafeMatcher<ReplicatorInstanceEvent>() {
      @Override
      protected boolean matchesSafely(ReplicatorInstanceEvent item) {
        return item.eventType == ReplicatorInstanceEvent.EventType.LEADER_ELECTED
            && leaderMatcher.matches(item.newLeader)
            && termMatcher.matches(item.leaderElectedTerm);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a ReplicatorInstanceEvent indicating a leader was elected")
            .appendText(" with id ").appendDescriptionOf(leaderMatcher)
            .appendText(" with term ").appendDescriptionOf(termMatcher);

      }
    };
  }

  static Matcher<ReplicatorInstanceEvent> aQuorumChangeCommittedEvent(QuorumConfiguration configuration,
                                                                      Matcher<Long> fromMatcher) {
    return new TypeSafeMatcher<ReplicatorInstanceEvent>() {
      @Override
      protected boolean matchesSafely(ReplicatorInstanceEvent item) {
        return item.eventType == ReplicatorInstanceEvent.EventType.QUORUM_CONFIGURATION_COMMITTED
            && fromMatcher.matches(item.instance.getId())
            && item.configuration.equals(configuration);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a ReplicatorInstanceEvent indicating quorum configuration ").appendValue(configuration)
            .appendText(" was committed from replicator with ID ").appendDescriptionOf(fromMatcher);
      }
    };
  }

  static Matcher<ReplicatorInstanceEvent> aReplicatorEvent(ReplicatorInstanceEvent.EventType type) {
    return new TypeSafeMatcher<ReplicatorInstanceEvent>() {
      @Override
      protected boolean matchesSafely(ReplicatorInstanceEvent item) {
        return item.eventType == type;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a ReplicatorInstanceEvent of type ").appendValue(type);
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

  static Matcher<InRamTest.PeerController> wonAnElectionWithTerm(Matcher<Long> termMatcher) {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        return peer.hasWonAnElection(termMatcher);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a peer who won an election with term ").appendDescriptionOf(termMatcher);
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

  static Matcher<InRamTest.PeerController> willCommitConfiguration(QuorumConfiguration configuration) {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        try {
          peer.waitForQuorumCommit(configuration);
        } catch (Exception e) {
          matchException = e;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Peer will commit the quorum configuration ").appendValue(configuration);
      }

      @Override
      public void describeMismatchSafely(InRamTest.PeerController peer, Description description) {
        if (matchException != null) {
          description.appendValue(matchException.toString());
        }
      }
    };
  }

  static Matcher<InRamTest.PeerController> willRespondToAnAppendRequest(long minimumTerm) {
    return new TypeSafeMatcher<InRamTest.PeerController>() {
      Throwable matchException;

      @Override
      protected boolean matchesSafely(InRamTest.PeerController peer) {
        try {
          peer.waitForAppendReply(greaterThanOrEqualTo(minimumTerm));
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

  static Matcher<List<LogEntry>> aListOfEntriesWithConsecutiveSeqNums(long start, long end) {
    return new TypeSafeMatcher<List<LogEntry>>() {
      @Override
      protected boolean matchesSafely(List<LogEntry> entries) {
        if (entries.size() != (end - start)) {
          return false;
        }
        long expectedIndex = start;
        for (LogEntry entry : entries) {
          if (entry.getIndex() != expectedIndex) {
            return false;
          }
          expectedIndex++;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a list of LogEntry with consecutive indexes from ")
            .appendValue(start).appendText(" inclusive to ").appendValue(end).appendText(" exclusive");
      }
    };
  }
}
