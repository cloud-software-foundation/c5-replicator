/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

package ohmdb.log;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import ohmdb.client.OhmConstants;
import ohmdb.generated.Log;
import ohmdb.replication.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OLog implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(OLog.class);
  private static final long RETRY_WAIT_TIME = 100;
  private final Path walDir;
  private final Path archiveLogPath;
  long fileNum;
  Path logPath;
  private FileOutputStream logOutputStream;

  public OLog(String basePath) throws IOException {
    this.fileNum = System.currentTimeMillis();
    this.walDir = Paths.get(basePath, OhmConstants.WAL_DIR);
    this.archiveLogPath = Paths.get(basePath, OhmConstants.ARCHIVE_DIR);
    this.logPath = Paths.get(walDir.toString(), OhmConstants.LOG_NAME + fileNum);

    if (!logPath.getParent().toFile().exists()) {
      boolean success = logPath.getParent().toFile().mkdirs();
      if (!success) {
        throw new IOException("Unable to setup logPath");
      }
    }

    if (!archiveLogPath.toFile().exists()) {
      boolean success = archiveLogPath.toFile().mkdirs();
      if (!success) {
        throw new IOException("Unable to setup archive Log Path");
      }
    }
    logOutputStream = new FileOutputStream(logPath.toFile(), true);
  }

  public static void moveAwayOldLogs(final String basePath)
      throws IOException {
    Path walPath = Paths.get(basePath, OhmConstants.WAL_DIR);
    Path archiveLogPath = Paths.get(basePath, OhmConstants.ARCHIVE_DIR);
    File[] files = walPath.toFile().listFiles();

    if (files == null) {
      return;
    }

    if (!archiveLogPath.toFile().exists()) {
      boolean success = archiveLogPath.toFile().mkdirs();
      if (!success) {
        throw new IOException("Unable to setup archive Log Path");
      }
    }

    for (File file : files) {
      boolean success = file.renameTo(archiveLogPath
          .resolve(file.getName())
          .toFile());
      if (!success) {
        String err = "Unable to move: " + file + " to " + archiveLogPath;
        throw new IOException(err);
      }
    }
  }

  public void sync(SettableFuture<Boolean> syncComplete) throws IOException {
    boolean success = false;
    do {
      try {
        this.logOutputStream.flush();
        this.logOutputStream.getChannel().force(false);
        success = true;
        syncComplete.set(true);
      } catch (ClosedChannelException e) {
        try {
          Thread.sleep(RETRY_WAIT_TIME);
        } catch (InterruptedException e1) {
          throw new IOException(e1);
        }
      }
    } while (!success);
  }

  @Override
  public void close() throws IOException {
    SettableFuture<Boolean> syncComplete = SettableFuture.create();
    this.sync(syncComplete);
    try {
      syncComplete.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
    this.logOutputStream.close();
  }

  public void roll() throws IOException, ExecutionException, InterruptedException {
    this.close();
    boolean success = logPath.toFile().renameTo(archiveLogPath
        .resolve(logPath.toFile().getName())
        .toFile());
    if (!success) {
      String err = "Unable to move: " + logPath + " to " + archiveLogPath;
      throw new IOException(err);
    }

    this.fileNum = System.currentTimeMillis();
    this.logPath = Paths.get(this.walDir.toString(), OhmConstants.LOG_NAME + fileNum);

    logOutputStream = new FileOutputStream(this.logPath.toFile(),
        true);
  }

  /**
   * Clean out old logs
   *
   * @param timestamp Only clear logs older than timestamp. Or if 0 then always
   *                  remove.
   * @throws IOException
   */
  public void clearOldLogs(long timestamp) throws IOException {
    File[] files = this.archiveLogPath.toFile().listFiles();
    if (files == null) {
      return;
    }

    for (File file : files) {
      LOG.debug("Removing old log file" + file);
      if (timestamp == 0 || file.lastModified() > timestamp) {
        boolean success = file.delete();
        if (!success) {
          throw new IOException("Unable to delete file:" + file);
        }
      }
    }
  }

  /**
   * @param index
   * @param nodeId
   * @return The log data for a node at an index or null if not found.
   */

  private Log.OLogEntry getLogDataFromDisk(long index, long nodeId)
      throws IOException, ExecutionException, InterruptedException {
    SettableFuture<Boolean> f = SettableFuture.create();
    this.sync(f);
    f.get();

    FileInputStream fileInputStream = new FileInputStream(logOutputStream.getFD());
    Log.OLogEntry nextEntry;

    do {
      // Handle tombestones TODO
      nextEntry = Log.OLogEntry.parseDelimitedFrom(fileInputStream);
      // EOF
      if (nextEntry == null) {
        return null;
      }
    } while (nextEntry.getNodeId() != nodeId && nextEntry.getIndex() != index);

    return nextEntry;
  }

  public ListenableFuture<Boolean> logEntry(List<Log.OLogEntry> entries, long raftId) {
    SettableFuture<Boolean> completionNotification = SettableFuture.create();
    try {
      for (Log.OLogEntry entry : entries) {
        entry.writeDelimitedTo(this.logOutputStream);
      }
      completionNotification.set(true);
      return completionNotification;
    } catch (IOException e) {
      completionNotification.setException(e);
      return completionNotification;
    }
  }

  public Raft.LogEntry getLogEntry(long index, long nodeId) {
    try {
      Log.OLogEntry ologEntry = getLogDataFromDisk(index, nodeId);
      return Raft
          .LogEntry
          .newBuilder()
          .setIndex(ologEntry.getIndex())
          .setTerm(ologEntry.getTerm())
          .setData(ologEntry.getValue())
          .build();

    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException("CRASH!");
    }
  }

  public long getLogTerm(long index, long nodeId) {
    try {
      return getLogDataFromDisk(index, nodeId).getTerm();
    } catch (IOException | ExecutionException | InterruptedException e) {

      throw new RuntimeException("CRASH!");
    }
  }

  public ListenableFuture<Boolean> truncateLog(long entryIndex, long raftId) {
    try {
      SettableFuture<Boolean> truncateComplete = SettableFuture.create();
      Log.OLogEntry entry = Log.OLogEntry.newBuilder()
          .setTombStone(true)
          .setIndex(entryIndex)
          .setNodeId(raftId).build();

      entry.writeDelimitedTo(this.logOutputStream);
      this.sync(truncateComplete);
      return truncateComplete;
    } catch (Exception e) {
      throw new RuntimeException("CRASH!");
    }
  }

}
