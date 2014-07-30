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

import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static c5db.log.ReplicatorLogGenericTestUtil.seqNum;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;

public class InMemoryPersistenceNavigatorTest {
  private static final int MAX_SEEK = 7;
  private static final int LAST_SEQ_NUM = 27;

  private final ByteArrayPersistence persistence = new ByteArrayPersistence();
  private final MethodCallCountingCodec navigatorsCodec = new MethodCallCountingCodec();
  private final InMemoryPersistenceNavigator<DummyEntry> navigator =
      new InMemoryPersistenceNavigator<>(persistence, navigatorsCodec);

  private final SequentialLog<DummyEntry> log = new EncodedSequentialLog<>(
      persistence,
      new MethodCallCountingCodec(),
      navigator);

  @Before
  public void configureNavigatorAndPopulateTheLogWithSomeEntries() throws Exception {
    navigator.setMaxEntrySeek(MAX_SEEK);
    log.append(someConsecutiveDummyEntries(1, LAST_SEQ_NUM + 1));
  }

  @Test
  public void neverDecodesAFullEntryWhileNavigating() throws Exception {
    performVariousNavigatorOperations();
    assertThat(navigatorsCodec.numDecodes, is(0));
  }

  @Test
  public void placesAnUpperBoundOnTheNumberOfEntriesItSkipsPastWhenComputingEntriesAddresses() throws Exception {
    for (int i = LAST_SEQ_NUM; i >= 1; i--) {
      final long seqNum = (long) i;
      int numberOfSkipOperations = numberOfSkipOperations(() -> navigator.getAddressOfEntry(seqNum));
      assertThat(numberOfSkipOperations, is(lessThanOrEqualTo(MAX_SEEK)));
    }
  }

  @Test
  public void cachesAddressOfLastEntry() throws Exception {
    tidyGetStreamAtLastEntry();
    int numberOfSkipOperationsForASecondCall = numberOfSkipOperations(() -> tidyGetStreamAtSeqNum(LAST_SEQ_NUM));
    assertThat(numberOfSkipOperationsForASecondCall, is(0));
  }

  @Test
  public void cachesAddressOfAPreviousEntryLookup() throws Exception {
    tidyGetStreamAtSeqNum(20);
    int numberOfSkipOperationsForASecondCall = numberOfSkipOperations(() -> tidyGetStreamAtSeqNum(20));
    assertThat(numberOfSkipOperationsForASecondCall, is(0));
  }

  @Test(expected = Exception.class)
  public void throwsAnExceptionIfAskedToTruncateToSeqNumZero() throws Exception {
    navigator.notifyTruncation(0);
  }

  @Test
  public void returnsAStreamPositionedAtTheFirstEntry() throws Exception {
    try (InputStream input = navigator.getStreamAtFirstEntry()) {
      assertThat(navigatorsCodec.decode(input).getSeqNum(), is(equalTo(1L)));
    }
  }

  @Test
  public void returnsAStreamPositionedAtTheLastEntry() throws Exception {
    try (InputStream input = navigator.getStreamAtLastEntry()) {
      assertThat(navigatorsCodec.decode(input).getSeqNum(), is(equalTo((long) LAST_SEQ_NUM)));
    }
  }

  @Test
  public void returnsAStreamPositionedAtASpecifiedEntry() throws Exception {
    long entrySeqNum = 12;

    try (InputStream input = navigator.getStreamAtSeqNum(entrySeqNum)) {
      assertThat(navigatorsCodec.decode(input).getSeqNum(), is(equalTo(entrySeqNum)));
    }
  }


  private void performVariousNavigatorOperations() throws Exception {
    tidyGetStreamAtSeqNum(20);
    tidyGetStreamAtSeqNum(15);
    tidyGetStreamAtLastEntry();
    navigator.getAddressOfEntry(6);
  }

  private void tidyGetStreamAtSeqNum(long seqNum) throws Exception {
    navigator.getStreamAtSeqNum(seqNum).close();
  }

  private void tidyGetStreamAtLastEntry() throws Exception {
    navigator.getStreamAtLastEntry().close();
  }

  private int numberOfSkipOperations(ExceptionRunnable navigationOperation) throws Exception {
    int initialSkipCount = navigatorsCodec.numSkips;
    navigationOperation.run();
    return navigatorsCodec.numSkips - initialSkipCount;
  }

  private static List<DummyEntry> someConsecutiveDummyEntries(int start, int end) {
    List<DummyEntry> entries = new ArrayList<>(end - start);
    for (int i = start; i < end; i++) {
      entries.add(new DummyEntry(seqNum(i)));
    }
    return entries;
  }

  private static class DummyEntry extends SequentialEntry {
    public DummyEntry(long seqNum) {
      super(seqNum);
    }
  }

  private class MethodCallCountingCodec implements SequentialEntryCodec<DummyEntry> {
    public int numDecodes = 0;
    public int numSkips = 0;

    @Override
    public ByteBuffer[] encode(DummyEntry entry) {
      ByteBuffer encoded = ByteBuffer.allocate(8).putLong(entry.getSeqNum());
      encoded.flip();
      return new ByteBuffer[]{encoded};
    }

    @Override
    public DummyEntry decode(InputStream inputStream) throws IOException {
      numDecodes++;
      return new DummyEntry(getNextLongFrom(inputStream));
    }

    @Override
    public long skipEntryAndReturnSeqNum(InputStream inputStream) throws IOException {
      numSkips++;
      return getNextLongFrom(inputStream);
    }

    private long getNextLongFrom(InputStream inputStream) throws IOException {
      return new DataInputStream(inputStream).readLong();
    }
  }

  private interface ExceptionRunnable {
    public void run() throws Exception;
  }
}
