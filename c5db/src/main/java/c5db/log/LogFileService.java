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

import c5db.C5ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

/**
 * LogPersistenceService using Files and FileChannels.
 */
public class LogFileService implements LogPersistenceService {
  private static final Logger LOG = LoggerFactory.getLogger(LogFileService.class);

  private final Path walDir;
  private final Path archiveDir;

  public static class FilePersistence implements BytePersistence {
    private final FileChannel appendChannel;
    private final File logFile;

    public FilePersistence(File logFile) throws IOException {
      appendChannel = FileChannel.open(logFile.toPath(), CREATE, APPEND);
      this.logFile = logFile;
    }

    @Override
    public long size() throws IOException {
      return appendChannel.position();
    }

    @Override
    public void append(ByteBuffer[] buffers) throws IOException {
      appendChannel.write(buffers);
    }

    @Override
    public PersistenceReader getReader() throws IOException {
      return new NioReader(FileChannel.open(logFile.toPath(), READ));
    }

    @Override
    public void truncate(long size) throws IOException {
      if (size > this.size()) {
        throw new IllegalArgumentException("Truncation may not grow the file");
      }
      appendChannel.truncate(size);
    }

    @Override
    public void sync() throws IOException {
      appendChannel.force(true);
    }

    @Override
    public void close() throws IOException {
      appendChannel.close();
    }
  }

  private static class NioReader implements PersistenceReader {
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

  public LogFileService(Path basePath) throws IOException {
    this.walDir = basePath.resolve(C5ServerConstants.WAL_DIR);
    this.archiveDir = basePath.resolve(C5ServerConstants.ARCHIVE_DIR);

    createDirectoryStructure();
  }

  @Override
  public BytePersistence getPersistence(String quorumId) throws IOException {
    File logFile = prepareNewLogFileOrFindExisting(quorumId);
    return new FilePersistence(logFile);
  }

  /**
   * Move everything in the log directory to the archive directory.
   *
   * @throws IOException
   */
  public void moveLogsToArchive() throws IOException {
    for (File file : allFilesInDirectory(walDir)) {
      boolean success = file.renameTo(archiveDir
          .resolve(file.getName())
          .toFile());
      if (!success) {
        String err = "Unable to move: " + file.getAbsolutePath() + " to " + walDir;
        throw new IOException(err);
      }
    }
  }

  /**
   * Clean out old logs.
   *
   * @param timestamp Only clear logs older than timestamp. Or if 0 then remove all logs.
   * @throws IOException
   */
  public void clearOldArchivedLogs(long timestamp) throws IOException {
    for (File file : allFilesInDirectory(archiveDir)) {
      if (timestamp == 0 || file.lastModified() > timestamp) {
        LOG.debug("Removing old log file" + file);
        boolean success = file.delete();
        if (!success) {
          throw new IOException("Unable to delete file:" + file);
        }
      }
    }
  }


  /**
   * Finding an existing write-ahead log file to use for the specified quorum, or create
   * a new one if an old one isn't found.
   */
  private File prepareNewLogFileOrFindExisting(String quorumId) {
    File logFile = findExistingLogFile(quorumId);
    if (logFile == null) {
      logFile = prepareNewLogFile(quorumId);
      assert logFile != null;
    }
    return logFile;
  }

  private File findExistingLogFile(String quorumId) {
    for (File file : allFilesInDirectory(walDir)) {
      if (file.getName().startsWith(logFilePrefix(quorumId))) {
        LOG.info("Existing WAL found {} ; using it", file);
        return file;
      }
    }
    return null;
  }

  private File prepareNewLogFile(String quorumId) {
    long fileNum = System.currentTimeMillis();
    return walDir.resolve(logFilePrefix(quorumId) + fileNum).toFile();
  }

  private static String logFilePrefix(String quorumId) {
    return C5ServerConstants.LOG_NAME + quorumId;
  }

  private void createDirectoryStructure() throws IOException {
    createDirsIfNecessary(walDir);
    createDirsIfNecessary(archiveDir);
  }

  private static File[] allFilesInDirectory(Path dirPath) {
    File[] files = dirPath.toFile().listFiles();
    if (files == null) {
      return new File[]{};
    } else {
      return files;
    }
  }

  /**
   * Create the directory at dirPath if it does not already exist.
   *
   * @param dirPath Path to directory to create.
   * @throws IOException
   */
  private static void createDirsIfNecessary(Path dirPath) throws IOException {
    if (!dirPath.toFile().exists()) {
      boolean success = dirPath.toFile().mkdirs();
      if (!success) {
        LOG.error("Creation of path {} failed", dirPath);
        throw new IOException("createDirsIfNecessary");
      }
    }
  }
}
