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

package c5db.util;

import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class CrcInputStreamTest {
  @Test
  public void calculatesACrcCorrectlyWhenReading() throws Exception {
    final byte[] data = getUtf8Bytes("The quick brown fox jumps over the \000\001\002\003");

    final long expectedCrc = computeCrcDirectly(data);

    final CrcInputStream crcInputStream = createCrcInputStreamReadingFromByteArray(data);
    readEntireInputStream(crcInputStream);

    assertThat(crcInputStream.getValue(), is(equalTo(expectedCrc)));
  }


  private static Checksum newChecksumObject() {
    return new Adler32();
  }

  private static long computeCrcDirectly(byte[] bytes) {
    Checksum crc = newChecksumObject();
    crc.update(bytes, 0, bytes.length);
    return crc.getValue();
  }

  private static CrcInputStream createCrcInputStreamReadingFromByteArray(byte[] bytes) {
    final InputStream inputStream = new ByteArrayInputStream(bytes);
    return new CrcInputStream(inputStream, newChecksumObject());
  }

  private static byte[] getUtf8Bytes(String string) {
    return string.getBytes(CharsetUtil.UTF_8);
  }

  private static void readEntireInputStream(InputStream inputStream) throws Exception {
    // Read 4 individual bytes, then do byte array reads until the end of the input.

    for (int i = 0; i < 4; i++) {
      //noinspection ResultOfMethodCallIgnored
      inputStream.read();
    }

    final byte[] buffer = new byte[3];
    int bytesRead;
    do {
      bytesRead = inputStream.read(buffer);
    } while (bytesRead > 0);
  }
}
