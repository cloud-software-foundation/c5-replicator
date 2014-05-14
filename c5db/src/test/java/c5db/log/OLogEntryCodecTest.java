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
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import static c5db.log.LogTestUtil.aSeqNum;
import static c5db.log.LogTestUtil.anElectionTerm;
import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class OLogEntryCodecTest {
  private final SequentialEntryCodec<OLogEntry> codec = new OLogEntry.Codec();
  private final PipedOutputStream pipedOutputStream = new PipedOutputStream();
  private InputStream readFromMe;
  private final WritableByteChannel writeToMe = Channels.newChannel(pipedOutputStream);

  @Before
  public void setUpIOPipe() throws Exception {
    readFromMe = new PipedInputStream(pipedOutputStream);
  }

  @Test
  public void decodesEntriesItEncodes() throws Exception {
    OLogEntry entryToEncode = anOLogEntry();
    ByteBuffer[] encodedBytes = codec.encode(entryToEncode);
    writeBuffersToPipe(encodedBytes, writeToMe);

    OLogEntry reconstructedEntry = codec.decode(readFromMe);
    assertThat(reconstructedEntry, is(equalTo(entryToEncode)));
  }

  @Test
  public void skipsEntriesItEncodes() throws Exception {
    OLogEntry entryToEncode = makeEntry(seqNum(33), term(44), "data");
    ByteBuffer[] encodedBytes = codec.encode(entryToEncode);
    writeBuffersToPipe(encodedBytes, writeToMe);

    long seqNum = codec.skipEntryAndReturnSeqNum(readFromMe);
    assertThat(seqNum, is(equalTo(33L)));
  }

  @Test
  public void decodesQuorumConfigurationEntriesItEncodes() throws Exception {
    final QuorumConfigurationMessage message = aQuorumConfigurationMessage();
    final OLogEntry configurationEntry = new OLogEntry(aSeqNum(), anElectionTerm(),
        new OLogProtostuffContent<>(message));

    ByteBuffer[] encodedBytes = codec.encode(configurationEntry);
    writeBuffersToPipe(encodedBytes, writeToMe);

    OLogEntry reconstructedEntry = codec.decode(readFromMe);
    assertThat(reconstructedEntry, is(equalTo(configurationEntry)));
  }


  private static OLogEntry anOLogEntry() {
    return makeEntry(seqNum(77), term(88), "data");
  }

  private static QuorumConfigurationMessage aQuorumConfigurationMessage() {
    return QuorumConfiguration
        .of(Lists.newArrayList(1L, 2L, 3L))
        .getTransitionalConfiguration(Lists.newArrayList(4L, 5L, 6L))
        .toProtostuff();
  }

  private static void writeBuffersToPipe(ByteBuffer[] buffers, WritableByteChannel byteChannel) throws Exception {
    for (ByteBuffer b : buffers) {
      byteChannel.write(b);
    }
  }
}
