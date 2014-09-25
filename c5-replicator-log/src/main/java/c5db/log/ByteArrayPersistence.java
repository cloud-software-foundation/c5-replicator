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

import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * A BytePersistence implementation entirely within memory, for testing. This implementation
 * does not write anything to disk. It is not designed with efficiency in mind, either;
 * it freely does full copies of the underlying byte array.
 */
class ByteArrayPersistence implements LogPersistenceService.BytePersistence {
  private ByteArrayOutputStream stream;
  private volatile boolean closed;

  public ByteArrayPersistence() {
    stream = new ByteArrayOutputStream();
  }

  /**
   * Alter the byte at the specified position. The purpose of this method is to test e.g. CRCs.
   */
  public void corrupt(int position) throws IOException {
    byte[] bytes = stream.toByteArray();
    bytes[position] = (byte) (bytes[position] ^ 0x01);
    stream = new ByteArrayOutputStream(bytes.length);
    stream.write(bytes);
  }

  @Override
  public boolean isEmpty() throws IOException {
    return stream.size() == 0;
  }

  @Override
  public long size() throws IOException {
    return stream.size();
  }

  @Override
  public void append(ByteBuffer[] buffers) throws IOException {
    ensureNotClosed();
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
    ensureNotClosed();
    int intSize = Ints.checkedCast(size);
    byte[] bytes = stream.toByteArray();
    stream = new ByteArrayOutputStream(intSize);
    stream.write(bytes, 0, intSize);
  }

  @Override
  public void sync() throws IOException {
    ensureNotClosed();
  }

  @Override
  public void close() throws IOException {
    // With ByteArrayOutputStream, this is actually a no-op:
    stream.close();
    closed = true;
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

  private void ensureNotClosed() throws IOException {
    if (closed) {
      throw new IOException("attempted to use ByteArrayPersistence, but it is closed");
    }
  }
}
