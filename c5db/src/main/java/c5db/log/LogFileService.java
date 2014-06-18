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
import c5db.util.CheckedSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * LogPersistenceService using FilePersistence objects (Files and FileChannels).
 */
public class LogFileService implements LogPersistenceService<FilePersistence> {
  private static final Logger LOG = LoggerFactory.getLogger(LogFileService.class);

  private final Path walDir;
  private final Path archiveDir;

  public LogFileService(Path basePath) throws IOException {
    this.walDir = basePath.resolve(C5ServerConstants.WAL_DIR);
    this.archiveDir = basePath.resolve(C5ServerConstants.ARCHIVE_DIR);

    createDirectoryStructure();
  }

  @Nullable
  @Override
  public FilePersistence getCurrent(String quorumId) throws IOException {
    return null;
  }

  @NotNull
  @Override
  public FilePersistence create(String quorumId) throws IOException {
    return null;
  }

  @Override
  public void append(String quorumId, @NotNull FilePersistence persistence) throws IOException {

  }

  @Override
  public void truncate(String quorumId) throws IOException {

  }

  @Override
  public Iterator<CheckedSupplier<FilePersistence, IOException>> iterator(String quorumId) {
    return null;
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
    long fileNum = System.nanoTime();
    return walDir.resolve(logFilePrefix(quorumId) + fileNum).toFile();
  }

  private static String logFilePrefix(String quorumId) {
    return C5ServerConstants.LOG_NAME + "-" + quorumId + "-";
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
