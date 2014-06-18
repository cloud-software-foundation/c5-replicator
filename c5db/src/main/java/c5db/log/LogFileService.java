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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * LogPersistenceService using FilePersistence objects (Files and FileChannels).
 */
public class LogFileService implements LogPersistenceService<FilePersistence> {
  private final Path walRootDir;
  private final Path archiveDir;

  public LogFileService(Path basePath) throws IOException {
    this.walRootDir = basePath.resolve(C5ServerConstants.WAL_ROOT_DIRECTORY_NAME);
    this.archiveDir = basePath.resolve(C5ServerConstants.ARCHIVE_DIR);

    createDirectoryStructure();
  }

  @Nullable
  @Override
  public FilePersistence getCurrent(String quorumId) throws IOException {
    final Path currentLink = getCurrentLink(quorumId);
    if (currentLink == null) {
      return null;
    } else {
      return new FilePersistence(Files.readSymbolicLink(currentLink));
    }
  }

  @NotNull
  @Override
  public FilePersistence create(String quorumId) throws IOException {
    return new FilePersistence(getNewLogFilePath(quorumId));
  }

  @Override
  public void append(String quorumId, @NotNull FilePersistence persistence) throws IOException {
    final Path currentLink = getCurrentLink(quorumId);
    final long linkId;

    if (currentLink == null) {
      linkId = 1;
    } else {
      linkId = linkIdOfFile(currentLink.toFile()) + 1;
    }

    Files.createSymbolicLink(pathForLinkId(linkId, quorumId), persistence.path);
  }

  @Override
  public void truncate(String quorumId) throws IOException {
    Files.delete(getCurrentLink(quorumId));
  }

  @Override
  public Iterator<CheckedSupplier<FilePersistence, IOException>> iterator(String quorumId) {
    return new Iterator<CheckedSupplier<FilePersistence, IOException>>() {
      Iterator<Path> linkPathIterator; // Initialization requires IO, so do it lazily

      @Override
      public boolean hasNext() {
        initializeUnderlyingIteratorIfNeeded();
        return linkPathIterator.hasNext();
      }

      @Override
      public CheckedSupplier<FilePersistence, IOException> next() {
        initializeUnderlyingIteratorIfNeeded();
        Path linkPath = linkPathIterator.next();
        return () -> new FilePersistence(Files.readSymbolicLink(linkPath));
      }

      private void initializeUnderlyingIteratorIfNeeded() {
        try {
          if (linkPathIterator == null) {
            linkPathIterator = getLinkPathMap(quorumId).descendingMap().values().iterator();
          }
        } catch (IOException e) {
          throw new IteratorIOException(e);
        }
      }
    };
  }

  /**
   * Move everything in the log directory to the archive directory.
   *
   * @throws IOException
   */
  public void moveLogsToArchive() throws IOException {
    for (File file : allFilesInDirectory(walRootDir)) {
      boolean success = file.renameTo(archiveDir
          .resolve(file.getName())
          .toFile());
      if (!success) {
        String err = "Unable to move: " + file.getAbsolutePath() + " to " + walRootDir;
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
        boolean success = file.delete();
        if (!success) {
          throw new IOException("Unable to delete file:" + file);
        }
      }
    }
  }

  private Path getNewLogFilePath(String quorumId) throws IOException {
    String fileName = String.valueOf(System.nanoTime());
    createQuorumDirectoryIfNeeded(quorumId);
    return logFileDir(quorumId).resolve(fileName);
  }

  @Nullable
  private Path getCurrentLink(String quorumId) throws IOException {
    Map.Entry<Long, Path> lastEntry = getLinkPathMap(quorumId).lastEntry();
    if (lastEntry == null) {
      return null;
    } else {
      return lastEntry.getValue();
    }
  }

  private NavigableMap<Long, Path> getLinkPathMap(String quorumId) throws IOException {
    NavigableMap<Long, Path> linkPathMap = new TreeMap<>();

    for (File file : allFilesInDirectory(quorumDir(quorumId))) {
      if (!Files.isSymbolicLink(file.toPath())) {
        continue;
      }

      long fileId = linkIdOfFile(file);
      linkPathMap.put(fileId, file.toPath());
    }

    return linkPathMap;
  }

  private long linkIdOfFile(File file) {
    return Long.parseLong(file.getName());
  }

  private Path pathForLinkId(long linkId, String quorumId) {
    return quorumDir(quorumId).resolve(String.valueOf(linkId));
  }

  private Path quorumDir(String quorumId) {
    return walRootDir.resolve(quorumId);
  }

  private Path logFileDir(String quorumId) {
    return quorumDir(quorumId).resolve(C5ServerConstants.WAL_LOG_FILE_SUBDIRECTORY_NAME);
  }

  private void createDirectoryStructure() throws IOException {
    Files.createDirectories(walRootDir);
  }

  private void createQuorumDirectoryIfNeeded(String quorumId) throws IOException {
    Files.createDirectories(quorumDir(quorumId));
    Files.createDirectories(logFileDir(quorumId));
  }

  private static File[] allFilesInDirectory(Path dirPath) throws IOException {
    File[] files = dirPath.toFile().listFiles();
    if (files == null) {
      return new File[]{};
    } else {
      return files;
    }
  }
}
