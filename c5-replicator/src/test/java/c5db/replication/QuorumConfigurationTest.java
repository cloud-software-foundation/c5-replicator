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

package c5db.replication;

import c5db.interfaces.replication.QuorumConfiguration;
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
    final QuorumConfiguration transitional = stableConfiguration.getTransitionalConfiguration(aDestinationPeerSet());
    final QuorumConfiguration destinationConfiguration = QuorumConfiguration.of(aDestinationPeerSet());

    assertThat(transitional.isTransitional, is(equalTo(true)));
    assertThat(transitional.getCompletedConfiguration(), is(equalTo(destinationConfiguration)));
  }

  @Test
  public void returnsBeforeAndAfterPeerSetsFromATransitionalConfiguration() {
    final QuorumConfiguration transitional =
        QuorumConfiguration.of(aPeerSet())
            .getTransitionalConfiguration(aDestinationPeerSet());

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
    return aStableConfiguration().getTransitionalConfiguration(Sets.newHashSet(3L, 4L, 5L, 6L, 7L, 8L, 9L));
  }
}
