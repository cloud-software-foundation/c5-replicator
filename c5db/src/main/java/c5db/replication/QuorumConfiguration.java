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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Immutable value type representing a configuration of which peers are members of a quorum.
 * It satisfies the following invariants: if transitionalConfiguration is true, then allPeers is
 * the union of prevPeers and nextPeers. If transitionalConfiguration is false, then prevPeers
 * and nextPeers are empty.
 */
public final class QuorumConfiguration {

  public final boolean transitionalConfiguration;

  private final Set<Long> allPeers;
  private final Set<Long> prevPeers;
  private final Set<Long> nextPeers;

  public static QuorumConfiguration of(Collection<Long> peerCollection) {
    return new QuorumConfiguration(peerCollection);
  }

  public QuorumConfiguration transitionTo(Collection<Long> newPeerCollection) {
    assert !transitionalConfiguration;

    return new QuorumConfiguration(allPeers, newPeerCollection);
  }

  public QuorumConfiguration completeTransition() {
    assert transitionalConfiguration;

    return new QuorumConfiguration(nextPeers);
  }

  public Set<Long> allPeers() {
    return allPeers;
  }

  public Set<Long> prevPeers() {
    return prevPeers;
  }

  public Set<Long> nextPeers() {
    return nextPeers;
  }

  /**
   * Determine if the peers in sourceSet include a majority of the peers in this configuration.
   */
  public boolean setContainsMajority(Set<Long> sourceSet) {
    if (transitionalConfiguration) {
      return setComprisesMajorityOfAnotherSet(sourceSet, prevPeers)
          && setComprisesMajorityOfAnotherSet(sourceSet, nextPeers);
    } else {
      return setComprisesMajorityOfAnotherSet(sourceSet, allPeers);
    }
  }

  /**
   * Given a map which tells the last acknowledged entry index for different peers, find the maximum
   * index value which is less than or equal to a majority of this configuration's peers' indexes.
   */
  public long calculateCommittedIndex(Map<Long, Long> peersLastAckedIndex) {
    if (transitionalConfiguration) {
      return Math.min(
          getGreatestIndexCommittedByMajority(prevPeers, peersLastAckedIndex),
          getGreatestIndexCommittedByMajority(nextPeers, peersLastAckedIndex));
    } else {
      return getGreatestIndexCommittedByMajority(allPeers, peersLastAckedIndex);
    }
  }


  private QuorumConfiguration(Collection<Long> peers) {
    this.transitionalConfiguration = false;
    allPeers = ImmutableSet.copyOf(peers);
    prevPeers = nextPeers = ImmutableSet.of();
  }

  private QuorumConfiguration(Collection<Long> prevPeers, Collection<Long> nextPeers) {
    this.transitionalConfiguration = true;
    this.prevPeers = ImmutableSet.copyOf(prevPeers);
    this.nextPeers = ImmutableSet.copyOf(nextPeers);
    this.allPeers = Sets.union(this.prevPeers, this.nextPeers).immutableCopy();
  }


  private static long getGreatestIndexCommittedByMajority(Set<Long> peers, Map<Long, Long> peersLastAckedIndex) {
    SortedMultiset<Long> committedIndexes = TreeMultiset.create();
    for (long peerId : peers) {
      committedIndexes.add(peersLastAckedIndex.getOrDefault(peerId, 0L));
    }
    return Iterables.get(committedIndexes.descendingMultiset(), calculateNumericalMajority(peers.size()) - 1);
  }

  private static <T> boolean setComprisesMajorityOfAnotherSet(Set<T> sourceSet, Set<T> destSet) {
    return Sets.intersection(sourceSet, destSet).size() >= calculateNumericalMajority(destSet.size());
  }

  private static int calculateNumericalMajority(int setSize) {
    return (setSize / 2) + 1;
  }

  @Override
  public String toString() {
    return "QuorumConfiguration{" +
        "transitionalConfiguration=" + transitionalConfiguration +
        ", allPeers=" + allPeers +
        ", prevPeers=" + prevPeers +
        ", nextPeers=" + nextPeers +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QuorumConfiguration that = (QuorumConfiguration) o;

    if (transitionalConfiguration != that.transitionalConfiguration) {
      return false;
    }
    if (!allPeers.equals(that.allPeers)) {
      return false;
    }
    if (!nextPeers.equals(that.nextPeers)) {
      return false;
    }
    return prevPeers.equals(that.prevPeers);
  }

  @Override
  public int hashCode() {
    int result = (transitionalConfiguration ? 1 : 0);
    result = 31 * result + allPeers.hashCode();
    result = 31 * result + prevPeers.hashCode();
    result = 31 * result + nextPeers.hashCode();
    return result;
  }
}
