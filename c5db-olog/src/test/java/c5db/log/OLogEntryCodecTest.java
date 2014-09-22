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

package c5db.log;

import c5db.interfaces.log.SequentialEntryCodec;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import static c5db.log.LogTestUtil.anOLogConfigurationEntry;
import static c5db.log.LogTestUtil.anOLogEntry;
import static c5db.log.LogTestUtil.makeEntry;
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
    final OLogEntry entryToEncode = anOLogEntry();

    havingEncodedAndWrittenEntry(entryToEncode);

    OLogEntry reconstructedEntry = codec.decode(readFromMe);
    assertThat(reconstructedEntry, is(equalTo(entryToEncode)));
  }

  @Test
  public void isAbleToSkipEntriesItDecodesAndToReturnTheSeqNumOfTheSkippedEntry() throws Exception {
    long seqNumOfEntry = 33;
    long arbitraryTerm = 44;

    OLogEntry entryToEncode = makeEntry(seqNumOfEntry, arbitraryTerm, "data");

    havingEncodedAndWrittenEntry(entryToEncode);

    long seqNum = codec.skipEntryAndReturnSeqNum(readFromMe);
    assertThat(seqNum, is(equalTo(seqNumOfEntry)));
  }

  @Test
  public void decodesQuorumConfigurationEntriesItEncodes() throws Exception {
    final OLogEntry configurationEntry = anOLogConfigurationEntry();

    havingEncodedAndWrittenEntry(configurationEntry);

    OLogEntry reconstructedEntry = codec.decode(readFromMe);
    assertThat(reconstructedEntry, is(equalTo(configurationEntry)));
  }


  private static void writeBuffersToPipe(ByteBuffer[] buffers, WritableByteChannel byteChannel) throws Exception {
    for (ByteBuffer b : buffers) {
      byteChannel.write(b);
    }
  }

  private void havingEncodedAndWrittenEntry(OLogEntry entry) throws Exception {
    ByteBuffer[] encodedBytes = codec.encode(entry);
    writeBuffersToPipe(encodedBytes, writeToMe);
  }
}
