package ohmdb;

import ohmdb.client.OhmConstants;
import ohmdb.log.OLogShim;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

public class FlushThread implements Runnable {
  int i = 0;

  private void flushAndCompact(int i) throws IOException {
    for (HRegion region : OnlineRegions.INSTANCE.regions()) {
      region.flushcache();
      region.getLog().rollWriter();
      if (i % OhmConstants.AMOUNT_OF_FLUSH_PER_COMPACT == 0) {
        region.compactStores();
      }
      if (i % OhmConstants.AMOUNT_OF_FLUSH_PER_OLD_LOG_CLEAR == 0) {
        ((OLogShim) region.getLog()).clearOldLogs(System.currentTimeMillis()
            - OhmConstants.OLD_LOG_CLEAR_AGE);
      }
    }
  }

  @Override
  public void run() {
    try {
      flushAndCompact(i++);
    } catch (IOException e) {
      throw new RuntimeException("CRASH");
    }
  }
}
