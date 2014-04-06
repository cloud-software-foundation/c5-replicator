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

import c5db.generated.OLogEntryHeader;
import io.protostuff.Schema;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EntryEncodingUtilTest {
  private static final Schema<OLogEntryHeader> SCHEMA = OLogEntryHeader.getSchema();
  private static final OLogEntryHeader TEST_ENTRY = new OLogEntryHeader(
      Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);

  private final PipedOutputStream pipedOutputStream = new PipedOutputStream();

  @Test
  public void decodesMessagesItEncodes() throws IOException {
    final InputStream readFromMe = new PipedInputStream(pipedOutputStream);
    final WritableByteChannel writeToMe = Channels.newChannel(pipedOutputStream);


    final List<ByteBuffer> serialized = encodeWithLengthAndCrc(SCHEMA, TEST_ENTRY);
    writeAllToChannel(serialized, writeToMe);

    final OLogEntryHeader decodedMessage = decodeAndCheckCrc(readFromMe, SCHEMA);

    assertThat(decodedMessage, is(theSameMessageAs(TEST_ENTRY)));
  }

  private static Matcher<OLogEntryHeader> theSameMessageAs(OLogEntryHeader message) {
    return new TypeSafeMatcher<OLogEntryHeader>() {
      @Override
      public boolean matchesSafely(OLogEntryHeader item) {
        return message.getSeqNum() == item.getSeqNum()
            && message.getTerm() == item.getTerm()
            && message.getContentLength() == item.getContentLength();
      }

      @Override
      public void describeTo(Description description) {
        description.appendValue(message);
      }
    };
  }

  private static void writeAllToChannel(List<ByteBuffer> buffers, WritableByteChannel channel) throws IOException {
    for (ByteBuffer buffer : buffers) {
      channel.write(buffer);
    }
  }
}
