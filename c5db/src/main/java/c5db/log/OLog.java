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

import c5db.client.C5Constants;
import c5db.generated.Log;
import c5db.replication.generated.Raft;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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

  public OLog(Path basePath) throws IOException {
      this.fileNum = System.currentTimeMillis();
      this.walDir = basePath.resolve(C5Constants.WAL_DIR);
      this.archiveLogPath = basePath.resolve(C5Constants.ARCHIVE_DIR);
      this.logPath = walDir.resolve(C5Constants.LOG_NAME + fileNum);


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

    public static void moveAwayOldLogs(final Path basePath)
            throws IOException {
        Path walPath = basePath.resolve(C5Constants.WAL_DIR);
        Path archiveLogPath = basePath.resolve(C5Constants.ARCHIVE_DIR);
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
                // TODO this needs to be processed on a background thread, not while the caller is waiting. Async notification.
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
        this.logPath = Paths.get(this.walDir.toString(), C5Constants.LOG_NAME + fileNum);

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
     * @param quorumId
     * @return The log data for a node at an index or null if not found.
     */

    private Log.OLogEntry getLogDataFromDisk(long index, String quorumId)
            throws IOException, ExecutionException, InterruptedException {
        SettableFuture<Boolean> f = SettableFuture.create();
        this.sync(f);
        f.get();

        FileInputStream fileInputStream = new FileInputStream(logOutputStream.getFD());
        Log.OLogEntry nextEntry;

        do {
            // TODO Handle tombestones
            try {
            nextEntry = Log.OLogEntry.parseDelimitedFrom(fileInputStream);
            } catch (IOException e) {
                return null;
            }
            // EOF
            if (nextEntry == null) {
                return null;
            }
        } while (!nextEntry.getQuorumId().equals(quorumId) && nextEntry.getIndex() != index);

        return nextEntry;
    }

    public ListenableFuture<Boolean> logEntry(List<Log.OLogEntry> entries, String quorumId) {
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

    public Raft.LogEntry getLogEntry(long index, String quorumId) {
        try {
            Log.OLogEntry ologEntry = getLogDataFromDisk(index, quorumId);
            return Raft
                    .LogEntry
                    .newBuilder()
                    .setIndex(ologEntry.getIndex())
                    .setTerm(ologEntry.getTerm())
                    .setData(ologEntry.getValue())
                    .build();

        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException("CRASH!", e);
        }
    }

    public long getLogTerm(long index, String quorumId) {
        try {
            Log.OLogEntry logEntry = getLogDataFromDisk(index, quorumId);
            if (logEntry == null) return 0;
            return logEntry.getTerm();
        } catch (IOException | ExecutionException | InterruptedException e) {

            throw new RuntimeException("CRASH!", e);
        }
    }

    public ListenableFuture<Boolean> truncateLog(long entryIndex, String quorumId) {
        try {
            SettableFuture<Boolean> truncateComplete = SettableFuture.create();
            Log.OLogEntry entry = Log.OLogEntry.newBuilder()
                    .setTombStone(true)
                    .setIndex(entryIndex)
                    .setQuorumId(quorumId).build();

            entry.writeDelimitedTo(this.logOutputStream);
            this.sync(truncateComplete);
            return truncateComplete;
        } catch (Exception e) {
            throw new RuntimeException("CRASH!");
        }
    }

}
