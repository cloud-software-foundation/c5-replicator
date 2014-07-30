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

import c5db.interfaces.log.SequentialEntryCodec;
import com.google.common.collect.Lists;
import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogTestUtil.anOLogEntry;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.nConsecutiveEntries;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.ReplicatorLogGenericTestUtil.seqNum;
import static c5db.log.ReplicatorLogGenericTestUtil.term;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("unchecked")
public class EncodedSequentialLogTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  private final BytePersistence persistence = context.mock(BytePersistence.class);
  private final SequentialEntryCodec<OLogEntry> codec = context.mock(SequentialEntryCodec.class);
  private final PersistenceNavigator navigator = context.mock(PersistenceNavigator.class);

  private final SequentialLog<OLogEntry> log = new EncodedSequentialLog<>(persistence, codec, navigator);

  @Test
  public void writesToTheSuppliedPersistenceObjectUsingTheSuppliedCodec() throws Exception {
    OLogEntry entry = makeEntry(seqNum(1), term(2), "data");

    context.checking(new Expectations() {{
      ignoring(navigator);
      allowing(persistence).size();

      oneOf(codec).encode(with(equalTo(entry)));
      atLeast(1).of(persistence).append(with(any(ByteBuffer[].class)));
    }});

    log.append(Lists.newArrayList(entry));
  }

  @Test
  public void notifiesTheNavigatorOnceForEveryEntryWritten() throws Exception {
    context.checking(new Expectations() {{
      ignoring(codec);
      ignoring(persistence);
      exactly(5).of(navigator).notifyLogging(with(any(Long.class)), with(any(Long.class)));
    }});

    log.append(nConsecutiveEntries(5));
  }

  @Test
  public void notifiesTheNavigatorWhenTruncating() throws Exception {
    long truncationSeqNum = 33;

    context.checking(new Expectations() {{
      ignoring(codec);
      ignoring(persistence);
      allowing(navigator).getAddressOfEntry(with(any(Long.class)));

      oneOf(navigator).notifyTruncation(truncationSeqNum);
    }});

    log.truncate(truncationSeqNum);
  }

  @Test
  public void readsEntriesFromTheSuppliedPersistenceObjectUsingTheSuppliedCodec() throws Exception {
    long startSeqNum = 77;
    long endSeqNum = 83;

    context.checking(new Expectations() {{
      codecWillReturnEntrySequence(codec, someConsecutiveEntries(startSeqNum, endSeqNum));
      allowing(persistence).getReader();
      allowing(navigator).getStreamAtSeqNum(startSeqNum);
      will(returnValue(aMockInputStream()));
    }});

    log.subSequence(startSeqNum, endSeqNum);
  }

  @Test
  public void processesTruncationRequestsByDelegatingThemToTheSuppliedPersistence() throws Exception {
    long entryAddress = 100;

    context.checking(new Expectations() {{
      allowing(navigator).notifyTruncation(with(any(Long.class)));

      oneOf(navigator).getAddressOfEntry(seqNum(7));
      will(returnValue(entryAddress));
      oneOf(persistence).truncate(entryAddress);
    }});

    log.truncate(seqNum(7));
  }

  @Test
  public void returnsNullWhenRequestedToGetTheLastEntryInAnEmptyLog() throws Exception {
    context.checking(new Expectations() {{
      oneOf(persistence).isEmpty();
      will(returnValue(true));
    }});

    assertThat(log.getLastEntry(), is(nullValue()));
  }

  @Test
  public void delegatesToItsNavigatorToReturnTheLastEntryInANonEmptyLog() throws Exception {
    final OLogEntry lastEntry = anOLogEntry();

    context.checking(new Expectations() {{
      oneOf(persistence).isEmpty();
      will(returnValue(false));

      oneOf(navigator).getStreamAtLastEntry();
      will(returnValue(aMockInputStream()));

      oneOf(codec).decode(with(any(InputStream.class)));
      will(returnValue(lastEntry));
    }});

    assertThat(log.getLastEntry(), equalTo(lastEntry));
  }


  private InputStream aMockInputStream() {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        return 0;
      }
    };
  }

  private void codecWillReturnEntrySequence(SequentialEntryCodec<OLogEntry> codec, List<OLogEntry> entries) throws Exception {
    Sequence seq = context.sequence("Codec#decode method call sequence");
    context.checking(new Expectations() {{
      for (OLogEntry e : entries) {
        oneOf(codec).decode(with(any(InputStream.class)));
        will(returnValue(e));
        inSequence(seq);
      }
    }});
  }

}
