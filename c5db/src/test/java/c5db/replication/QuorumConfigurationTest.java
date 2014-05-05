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

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class QuorumConfigurationTest {
  private final QuorumConfiguration stableConfiguration = aStableConfiguration();
  private final QuorumConfiguration transitionalConfiguration = aTransitionalConfiguration();

  @Test
  public void convertsAStableConfigurationToAndFromProtostuffFormat() {
    assertThat(
        QuorumConfiguration.fromProtostuff(
            stableConfiguration.toProtostuff()),
        is(equalTo(stableConfiguration)));
  }

  @Test
  public void convertsATransitionalConfigurationToAndFromProtostuffFormat() {
    assertThat(
        QuorumConfiguration.fromProtostuff(
            transitionalConfiguration.toProtostuff()),
        is(equalTo(transitionalConfiguration)));
  }

  @Test
  public void returnsNewConfigurationsRepresentingTransitionsToADifferentPeerSet() {
    final QuorumConfiguration transitional = stableConfiguration.transitionTo(aDestinationPeerSet());
    final QuorumConfiguration destinationConfiguration = QuorumConfiguration.of(aDestinationPeerSet());

    assertThat(transitional.transitionalConfiguration, is(equalTo(true)));
    assertThat(transitional.completeTransition(), is(equalTo(destinationConfiguration)));
  }

  @Test
  public void returnsBeforeAndAfterPeerSetsFromATransitionalConfiguration() {
    final QuorumConfiguration transitional =
        QuorumConfiguration.of(aPeerSet())
            .transitionTo(aDestinationPeerSet());

    assertThat(transitional.prevPeers(), equalTo(aPeerSet()));
    assertThat(transitional.nextPeers(), equalTo(aDestinationPeerSet()));
  }


  private Set<Long> aPeerSet() {
    return Sets.newHashSet(1L, 2L, 3L, 4L, 5L);
  }

  private QuorumConfiguration aStableConfiguration() {
    return QuorumConfiguration.of(aPeerSet());
  }

  private Set<Long> aDestinationPeerSet() {
    return Sets.newHashSet(3L, 4L, 5L, 6L);
  }

  private QuorumConfiguration aTransitionalConfiguration() {
    return aStableConfiguration().transitionTo(Sets.newHashSet(3L, 4L, 5L, 6L, 7L, 8L, 9L));
  }
}
