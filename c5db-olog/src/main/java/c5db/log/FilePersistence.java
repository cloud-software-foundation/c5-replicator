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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static c5db.log.LogPersistenceService.BytePersistence;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

/**
 * A BytePersistence using a File, accessed by FileChannels.
 */
public class FilePersistence implements BytePersistence {
  private final FileChannel appendChannel;
  final Path path;
  private long filePosition;

  public FilePersistence(Path path) throws IOException {
    this.path = path;
    appendChannel = FileChannel.open(path, CREATE, APPEND);
    filePosition = appendChannel.position();
  }

  @Override
  public boolean isEmpty() throws IOException {
    return filePosition == 0;
  }

  @Override
  public long size() throws IOException {
    return filePosition;
  }

  @Override
  public void append(ByteBuffer[] buffers) throws IOException {
    appendChannel.write(buffers);
    filePosition += totalBytesToBeWritten(buffers);
  }

  @Override
  public LogPersistenceService.PersistenceReader getReader() throws IOException {
    return new NioReader(FileChannel.open(path, READ));
  }

  @Override
  public void truncate(long size) throws IOException {
    if (size > this.size()) {
      throw new IllegalArgumentException("Truncation may not grow the file");
    }
    appendChannel.truncate(size);
    filePosition = size;
  }

  @Override
  public void sync() throws IOException {
    appendChannel.force(true);
  }

  @Override
  public void close() throws IOException {
    appendChannel.close();
  }

  // TODO This should be done once, in one central place
  private long totalBytesToBeWritten(ByteBuffer[] buffers) {
    long sum = 0;
    for (ByteBuffer b : buffers) {
      sum += b.position();
    }
    return sum;
  }

  private static class NioReader implements LogPersistenceService.PersistenceReader {
    private final FileChannel fileChannel;

    public NioReader(FileChannel fileChannel) {
      this.fileChannel = fileChannel;
    }

    @Override
    public long position() throws IOException {
      return fileChannel.position();
    }

    public void position(long newPos) throws IOException {
      fileChannel.position(newPos);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      return fileChannel.read(dst);
    }

    @Override
    public boolean isOpen() {
      return fileChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
      fileChannel.close();
    }
  }
}
