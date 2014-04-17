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

import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import static c5db.log.LogTestUtil.makeEntry;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class OLogEntryCodecTest {
  SequentialEntryCodec<OLogEntry> codec = new OLogEntry.Codec();
  private final PipedOutputStream pipedOutputStream = new PipedOutputStream();
  private InputStream readFromMe;
  final WritableByteChannel writeToMe = Channels.newChannel(pipedOutputStream);

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

  private static OLogEntry anOLogEntry() {
    return makeEntry(seqNum(77), term(88), "data");
  }

  private static void writeBuffersToPipe(ByteBuffer[] buffers, WritableByteChannel byteChannel) throws Exception {
    for (ByteBuffer b : buffers) {
      byteChannel.write(b);
    }
  }
}
