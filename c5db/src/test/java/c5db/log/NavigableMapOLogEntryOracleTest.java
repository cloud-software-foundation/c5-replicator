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

package c5db.log;

import c5db.replication.QuorumConfiguration;
import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import org.junit.Test;

import static c5db.log.LogTestUtil.entries;
import static c5db.log.OLogEntryOracle.QuorumConfigurationWithSeqNum;
import static c5db.replication.QuorumConfiguration.EMPTY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class NavigableMapOLogEntryOracleTest {
  private final NavigableMapOLogEntryOracle oracle = new NavigableMapOLogEntryOracle();

  private final QuorumConfiguration firstConfig = QuorumConfiguration.of(Lists.newArrayList(1L));
  private final QuorumConfiguration secondConfig = firstConfig.getTransitionalConfiguration(Lists.newArrayList(2L));

  @Test
  public void returnsTheElectionTermAtAGivenSeqNum() throws Exception {
    havingLogged(
        entries()
            .term(17).indexes(5, 6, 7)
            .term(18).indexes(8, 9, 10));

    assertThat(oracle.getTermAtSeqNum(4), is(equalTo(0L)));
    assertThat(oracle.getTermAtSeqNum(5), is(equalTo(17L)));
    assertThat(oracle.getTermAtSeqNum(10), is(equalTo(18L)));
  }

  @Test
  public void handlesTruncationsAndUpdatesTermInformationAccordingly() throws Exception {
    havingLogged(
        entries()
            .term(7).indexes(1, 2));
    havingTruncatedToIndex(2);
    havingLogged(
        entries()
            .term(8).indexes(2));

    assertThat(oracle.getTermAtSeqNum(1), is(equalTo(7L)));
    assertThat(oracle.getTermAtSeqNum(2), is(equalTo(8L)));
  }

  @Test
  public void returnsTheLastQuorumConfigurationAndItsSeqNum() throws Exception {
    havingLogged(
        entries()
            .term(999)
            .indexes(5)
            .configurationAndIndex(firstConfig, 6)
            .indexes(7, 8, 9));
    assertThat(oracle.getLastQuorumConfig(), is(equalTo(configurationAndIndex(firstConfig, 6))));

    havingLogged(
        entries()
            .term(999)
            .configurationAndIndex(secondConfig, 10));

    assertThat(oracle.getLastQuorumConfig(), is(equalTo(configurationAndIndex(secondConfig, 10))));
  }

  @Test
  public void handlesTruncationsAndUpdatesQuorumConfigurationInformationAccordingly() throws Exception {
    havingLogged(
        entries()
            .term(7).configurationAndIndex(firstConfig, 1));
    havingTruncatedToIndex(1);
    assertThat(oracle.getLastQuorumConfig(), is(equalTo(configurationAndIndex(EMPTY, 0))));

    havingLogged(
        entries()
            .term(8).configurationAndIndex(secondConfig, 2));
    assertThat(oracle.getLastQuorumConfig(), is(equalTo(configurationAndIndex(secondConfig, 2))));
  }


  private void havingLogged(LogTestUtil.LogSequenceBuilder sequenceBuilder) {
    for (LogEntry entry : sequenceBuilder.build()) {
      OLogEntry oLogEntry = OLogEntry.fromProtostuff(entry);
      oracle.notifyLogging(oLogEntry);
    }
  }

  private void havingTruncatedToIndex(long index) {
    oracle.notifyTruncation(index);
  }

  private QuorumConfigurationWithSeqNum configurationAndIndex(QuorumConfiguration quorumConfiguration,
                                                              long index) {
    return new QuorumConfigurationWithSeqNum(quorumConfiguration, index);
  }
}
