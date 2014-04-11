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

import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Write-ahead log "persistence" implementation entirely within memory, for testing.
 * This implementation does not write anything to disk. It is not designed with
 * efficiency in mind, either; it freely does full copies of the underlying byte array.
 */
class ByteArrayPersistence implements LogPersistenceService.BytePersistence {
  private ByteArrayOutputStream stream;

  public ByteArrayPersistence() {
    stream = new ByteArrayOutputStream();
  }

  public void overwrite(int position, int b) throws IOException {
    byte[] bytes = stream.toByteArray();
    bytes[position] = (byte) b;
    stream = new ByteArrayOutputStream(bytes.length);
    stream.write(bytes);
  }

  @Override
  public long size() throws IOException {
    return stream.size();
  }

  @Override
  public void append(ByteBuffer[] buffers) throws IOException {
    for (ByteBuffer buffer : buffers) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      stream.write(bytes);
    }
  }

  @Override
  public LogPersistenceService.PersistenceReader getReader() {
    return new ByteArrayPersistenceReader(stream.toByteArray());
  }

  @Override
  public void truncate(long size) throws IOException {
    int intSize = Ints.checkedCast(size);
    byte[] bytes = stream.toByteArray();
    stream = new ByteArrayOutputStream(intSize);
    stream.write(bytes, 0, intSize);
  }

  @Override
  public void sync() {
    // Not necessary to do anything here
  }

  @Override
  public void close() throws IOException {
    // With ByteArrayOutputStream, this is actually a no-op:
    stream.close();
  }


  public static class ByteArrayPersistenceReader implements LogPersistenceService.PersistenceReader {
    private final CountingInputStream stream;
    private final ReadableByteChannel channel;

    public ByteArrayPersistenceReader(byte[] bytes) {
      stream = new CountingInputStream(new ByteArrayInputStream(bytes));
      stream.mark(bytes.length);
      channel = Channels.newChannel(stream);
    }

    @Override
    public long position() throws IOException {
      return stream.getCount();
    }

    @Override
    public void position(long newPos) throws IOException {
      stream.reset();
      long actualSkipped = stream.skip(newPos);
      if (actualSkipped != newPos) {
        throw new IllegalArgumentException("Trying to set the reader position beyond the end of the readable bytes");
      }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      return channel.read(dst);
    }

    @Override
    public boolean isOpen() {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
