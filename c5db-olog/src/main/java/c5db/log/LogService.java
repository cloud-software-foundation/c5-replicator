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
import c5db.interfaces.LogModule;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.messages.generated.ModuleType;
import c5db.util.FiberSupplier;
import c5db.util.KeySerializingExecutor;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.fibers.Fiber;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * The Log module.
 */
public class LogService extends AbstractService implements LogModule {
  private final Path basePath;
  private final FiberSupplier fiberSupplier;

  // This map may only be read or written from tasks running on the fiber.
  private final Map<String, Mooring> moorings = new HashMap<>();

  private LogFileService logFileService;
  private OLog oLog;
  private Fiber fiber;

  public LogService(Path basePath, FiberSupplier fiberSupplier) {
    this.basePath = basePath;
    this.fiberSupplier = fiberSupplier;
  }

  @Override
  protected void doStart() {
    try {
      this.fiber = fiberSupplier.getFiber(this::failModule);
      this.logFileService = new LogFileService(basePath);
      KeySerializingExecutor executor = new WrappingKeySerializingExecutor(
          Executors.newFixedThreadPool(LogConstants.LOG_THREAD_POOL_SIZE));
      this.oLog = new QuorumDelegatingLog(
          logFileService,
          executor,
          NavigableMapOLogEntryOracle::new,
          InMemoryPersistenceNavigator::new);

      // TODO start the flush threads as necessary
      // TODO log maintenance threads can go here too.
      fiber.start();
      notifyStarted();
    } catch (IOException e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      dispose();
    } catch (IOException ignored) {
    } finally {
      notifyStopped();
    }
  }

  @Override
  public ListenableFuture<ReplicatorLog> getReplicatorLog(String quorumId) {
    SettableFuture<ReplicatorLog> logFuture = SettableFuture.create();

    fiber.execute(() -> {
      if (moorings.containsKey(quorumId)) {
        logFuture.set(moorings.get(quorumId));
        return;
      }

      try {
        // TODO this blocks on a fiber, and should be changed to use a callback.
        Mooring mooring = new Mooring(oLog, quorumId);
        moorings.put(quorumId, mooring);
        logFuture.set(mooring);

      } catch (IOException e) {
        logFuture.setException(e);
      }
    });

    return logFuture;
  }

  @Override
  public <E extends SequentialEntry> OLogReader<E> getLogReader(String quorumId, SequentialEntryCodec<E> entryCodec) {
    return new OLogReader<>(entryCodec, logFileService, quorumId);
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.Log;
  }

  @Override
  public boolean hasPort() {
    return false;
  }

  @Override
  public int port() {
    return 0;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }

  protected void failModule(Throwable t) {
    try {
      dispose();
    } catch (IOException ignore) {
    } finally {
      notifyFailed(t);
    }
  }

  private void dispose() throws IOException {
    oLog.close();
    oLog = null;

    fiber.dispose();
    fiber = null;

    logFileService = null;
  }

  @SuppressWarnings("UnusedDeclaration")
  public class FlushThread implements Runnable {
    int i = 0;

    private void flushAndCompact(int i) throws IOException {
//        for (HRegion region : OnlineRegions.INSTANCE.regions()) {
//          region.flushcache();
//          region.getLog().rollWriter();
//          if (i % C5Constants.AMOUNT_OF_FLUSH_PER_COMPACT == 0) {
//            region.compactStores();
//          }
//          if (i % C5Constants.AMOUNT_OF_FLUSH_PER_OLD_LOG_CLEAR == 0) {
//            olog.clearOldLogs(System.currentTimeMillis()
//                    - C5Constants.OLD_LOG_CLEAR_AGE);
//          }
//        }
    }

    @Override
    public void run() {
      try {
        flushAndCompact(i++);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
