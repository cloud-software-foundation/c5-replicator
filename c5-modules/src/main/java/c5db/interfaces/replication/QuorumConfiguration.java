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

package c5db.interfaces.replication;

import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Immutable value type representing a configuration of which peers are members of a quorum.
 * It satisfies the following invariants: if isTransitional is true, then allPeers is
 * the union of prevPeers and nextPeers. If isTransitional is false, then prevPeers
 * and nextPeers are empty.
 */
public final class QuorumConfiguration {

  public final boolean isTransitional;

  private final Set<Long> allPeers;
  private final Set<Long> prevPeers;
  private final Set<Long> nextPeers;

  public static final QuorumConfiguration EMPTY = new QuorumConfiguration(new HashSet<>());

  public static QuorumConfiguration of(Collection<Long> peerCollection) {
    return new QuorumConfiguration(peerCollection);
  }

  public static QuorumConfiguration fromProtostuff(c5db.replication.generated.QuorumConfigurationMessage message) {
    if (message.getTransitional()) {
      return new QuorumConfiguration(message.getPrevPeersList(), message.getNextPeersList());
    } else {
      return new QuorumConfiguration(message.getAllPeersList());
    }
  }

  public QuorumConfiguration getTransitionalConfiguration(Collection<Long> newPeerCollection) {
    if (isTransitional) {
      return new QuorumConfiguration(prevPeers, newPeerCollection);
    } else {
      return new QuorumConfiguration(allPeers, newPeerCollection);
    }
  }

  public QuorumConfiguration getCompletedConfiguration() {
    assert isTransitional;

    return new QuorumConfiguration(nextPeers);
  }

  public QuorumConfigurationMessage toProtostuff() {
    return new QuorumConfigurationMessage(
        isTransitional,
        Lists.newArrayList(allPeers),
        Lists.newArrayList(prevPeers),
        Lists.newArrayList(nextPeers));
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

  public boolean isEmpty() {
    return allPeers.size() == 0
        && prevPeers.size() == 0
        && nextPeers.size() == 0;
  }

  /**
   * Determine if the peers in sourceSet include a majority of the peers in this configuration.
   */
  public boolean setContainsMajority(Set<Long> sourceSet) {
    if (isTransitional) {
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
    if (isTransitional) {
      return Math.min(
          getGreatestIndexCommittedByMajority(prevPeers, peersLastAckedIndex),
          getGreatestIndexCommittedByMajority(nextPeers, peersLastAckedIndex));
    } else {
      return getGreatestIndexCommittedByMajority(allPeers, peersLastAckedIndex);
    }
  }


  private QuorumConfiguration(Collection<Long> peers) {
    this.isTransitional = false;
    allPeers = ImmutableSet.copyOf(peers);
    prevPeers = nextPeers = ImmutableSet.of();
  }

  private QuorumConfiguration(Collection<Long> prevPeers, Collection<Long> nextPeers) {
    this.isTransitional = true;
    this.prevPeers = ImmutableSet.copyOf(prevPeers);
    this.nextPeers = ImmutableSet.copyOf(nextPeers);
    this.allPeers = Sets.union(this.prevPeers, this.nextPeers).immutableCopy();
  }


  private static long getGreatestIndexCommittedByMajority(Set<Long> peers, Map<Long, Long> peersLastAckedIndex) {
    SortedMultiset<Long> committedIndexes = TreeMultiset.create();
    committedIndexes.addAll(peers.stream().map(peerId
        -> peersLastAckedIndex.getOrDefault(peerId, 0L)).collect(Collectors.toList()));
    return Iterables.get(committedIndexes.descendingMultiset(), calculateNumericalMajority(peers.size()) - 1);
  }

  private static <T> boolean setComprisesMajorityOfAnotherSet(Set<T> sourceSet, Set<T> destinationSet) {
    return Sets.intersection(sourceSet, destinationSet).size() >= calculateNumericalMajority(destinationSet.size());
  }

  private static int calculateNumericalMajority(int setSize) {
    return (setSize / 2) + 1;
  }

  @Override
  public String toString() {
    return "QuorumConfiguration{" +
        "isTransitional=" + isTransitional +
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

    return isTransitional == that.isTransitional
        && allPeers.equals(that.allPeers)
        && nextPeers.equals(that.nextPeers)
        && prevPeers.equals(that.prevPeers);
  }

  @Override
  public int hashCode() {
    int result = (isTransitional ? 1 : 0);
    result = 31 * result + allPeers.hashCode();
    result = 31 * result + prevPeers.hashCode();
    result = 31 * result + nextPeers.hashCode();
    return result;
  }
}
