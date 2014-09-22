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

import c5db.generated.OLogContentType;
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
  private static final OLogEntryHeader TEST_ENTRY =
      new OLogEntryHeader(Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, OLogContentType.DATA);

  private final PipedOutputStream pipedOutputStream = new PipedOutputStream();

  @Test
  public void decodesProtostuffMessagesItEncodes() throws IOException {
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
