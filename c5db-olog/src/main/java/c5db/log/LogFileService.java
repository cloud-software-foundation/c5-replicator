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

import c5db.LogConstants;
import c5db.util.CheckedSupplier;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * LogPersistenceService using FilePersistence objects (Files and FileChannels).
 */
public class LogFileService implements LogPersistenceService<FilePersistence> {
  private final Path logRootDir;

  public LogFileService(Path basePath) throws IOException {
    this.logRootDir = basePath.resolve(LogConstants.LOG_ROOT_DIRECTORY_RELATIVE_PATH);

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
  public ImmutableList<CheckedSupplier<FilePersistence, IOException>> getList(String quorumId) throws IOException {

    ImmutableList.Builder<CheckedSupplier<FilePersistence, IOException>> persistenceSupplierBuilder =
        ImmutableList.builder();

    for (Path path : getLinkPathMap(quorumId).descendingMap().values()) {
      persistenceSupplierBuilder.add(
          () -> new FilePersistence(Files.readSymbolicLink(path)));
    }

    return persistenceSupplierBuilder.build();
  }

  /**
   * Delete all the logs stored in the wal root directory.
   *
   * @throws IOException
   */
  public void clearAllLogs() throws IOException {
    Files.walkFileTree(logRootDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (!attrs.isDirectory()) {
          Files.delete(file);
        }
        return FileVisitResult.CONTINUE;
      }
    });
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
    return logRootDir.resolve(quorumId);
  }

  private Path logFileDir(String quorumId) {
    return quorumDir(quorumId).resolve(LogConstants.LOG_FILE_SUBDIRECTORY_RELATIVE_PATH);
  }

  private void createDirectoryStructure() throws IOException {
    Files.createDirectories(logRootDir);
  }

  private void createQuorumDirectoryIfNeeded(String quorumId) throws IOException {
    Files.createDirectories(quorumDir(quorumId));
    Files.createDirectories(logFileDir(quorumId));
  }

  private static File[] allFilesInDirectory(Path dirPath) {
    File[] files = dirPath.toFile().listFiles();
    if (files == null) {
      return new File[]{};
    } else {
      return files;
    }
  }
}
