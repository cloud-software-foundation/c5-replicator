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

import ohmdb.client.OhmConstants;
import ohmdb.generated.Log;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class OLog implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(OLog.class);
  private static final long RETRY_WAIT_TIME = 100;

  long fileNum;

  private final Path walDir;
  private final Path archiveLogPath;
  Path logPath;

  private FileOutputStream fileOutputStream;

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

    fileOutputStream = new FileOutputStream(logPath.toFile(), true);

  }

  protected void addEdit(Log.Entry edit) {
    boolean success = false;
    do {
      try {
        if (Arrays.equals(edit.getFamily().toByteArray(),
            Bytes.toBytes("METAFAMILY"))) {
          return;
        }

        edit.writeDelimitedTo(fileOutputStream);
        success = true;
      } catch (IOException | IllegalStateException e) {
        try {
          Thread.sleep(RETRY_WAIT_TIME);
        } catch (InterruptedException e1) {
          System.err.println(e);
          System.exit(1);
        }
      }
    } while (!success);
  }

  public void sync() throws IOException {
    boolean success = false;
    do {
      try {
        this.fileOutputStream.flush();
        this.fileOutputStream.getChannel().force(false);
        success = true;
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
    this.sync();
    this.fileOutputStream.close();
  }

  public void roll() throws IOException {
    this.close();
    boolean success = logPath.toFile().renameTo(archiveLogPath
        .resolve(logPath.toFile().getName())
        .toFile());
    if (!success) {
      String err = "Unable to move: " + logPath + " to " + archiveLogPath;
      throw new IOException(err);
    }

    this.fileNum = System.currentTimeMillis();
    this.logPath = Paths.get(this.walDir.toString(),
        OhmConstants.LOG_NAME + fileNum);

    fileOutputStream = new FileOutputStream(this.logPath.toFile(),
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
}
