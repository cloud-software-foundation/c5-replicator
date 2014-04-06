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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

/**
 * InputStream decorator which computes a running CRC on every byte read. The CRC object is
 * passed in on construction, and its value can be obtained any time. This class does no buffering
 * on its input.
 */
@SuppressWarnings("NullableProblems")
public class CrcInputStream extends FilterInputStream {
  private Checksum crc;

  public CrcInputStream(InputStream inputStream, Checksum crc) {
    super(inputStream);
    this.crc = crc;
  }

  public long getValue() {
    return crc.getValue();
  }

  public void reset() {
    crc.reset();
  }

  @Override
  public int read() throws IOException {
    int b = super.read();
    crc.update(b);
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = super.read(b, off, len);
    if (bytesRead > 0) {
      crc.update(b, off, bytesRead);
    }
    return bytesRead;
  }
}
