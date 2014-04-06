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

    assertThat("CRC computed while reading", crcInputStream.getValue(), is(equalTo(expectedCrc)));
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
