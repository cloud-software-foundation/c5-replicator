package ohmdb;

import ohmdb.client.OhmConstants;
import ohmdb.generated.Log;
import ohmdb.log.OLogShim;
import ohmdb.regionserver.RegistryFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class OhmStatic {
  private final static OnlineRegions onlineRegions = OnlineRegions.INSTANCE;

  static String getRandomPath() {
    Random random = new Random();
    return OhmConstants.TMP_DIR + random.nextInt();
  }

  static OLogShim recoverOhmServer(Configuration conf,
                                   Path path,
                                   RegistryFile registryFile)
      throws IOException {
    Map<HRegionInfo, List<HColumnDescriptor>> registry
        = registryFile.getRegistry();

    OLogShim hLog = new OLogShim(path.toString());
    for (HRegionInfo regionInfo : registry.keySet()) {
      HTableDescriptor hTableDescriptor =
          new HTableDescriptor(regionInfo.getTableName());

      for (HColumnDescriptor cf : registry.get(regionInfo)) {
        hTableDescriptor.addFamily(cf);
      }

      HRegion region = HRegion.openHRegion(new org.apache.hadoop.fs.Path(path.toString()),
          regionInfo,
          hTableDescriptor,
          hLog,
          conf,
          null,
          null);
      onlineRegions.put(region);
    }

    logReplay(path);
    hLog.clearOldLogs(0);
    return hLog;
  }

  static OLogShim bootStrapRegions(Configuration conf,
                                   Path path,
                                   RegistryFile registryFile) throws IOException {
    String tableName = "tableName";
    byte[] startKey = {0};
    byte[] endKey = {};

    HRegion region;
    HRegionInfo hRegionInfo = new HRegionInfo(Bytes.toBytes(tableName),
        startKey,
        endKey);

    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
    hTableDescriptor.addFamily(new HColumnDescriptor("cf"));

    OLogShim hlog = new OLogShim(path.toString());
    region = HRegion.createHRegion(hRegionInfo,
        new org.apache.hadoop.fs.Path(path.toString()),
        conf,
        hTableDescriptor,
        hlog);
    registryFile.addEntry(hRegionInfo, new HColumnDescriptor("cf"));
    onlineRegions.put(region);
    return hlog;
  }

  static boolean existingRegister(RegistryFile registryFile)
      throws IOException {
    return registryFile.getRegistry().size() != 0;
  }

  private static void logReplay(final Path path) throws IOException {
    java.nio.file.Path archiveLogPath = Paths.get(path.toString(),
        OhmConstants.ARCHIVE_DIR);
    File[] archiveLogs = archiveLogPath.toFile().listFiles();

    if (archiveLogs == null) {
      return;
    }

    for (File log : archiveLogs) {
      FileInputStream rif = new FileInputStream(log);
      processLogFile(rif);
      for (HRegion r : onlineRegions.regions()) {
        r.flushcache();
      }
    }
    for (HRegion r : onlineRegions.regions()) {
      r.compactStores();
    }

    for (HRegion r : onlineRegions.regions()) {
      r.waitForFlushesAndCompactions();
    }

    //TODO WE SHOULDN"T BE ONLINE TIL THIS HAPPENS
  }

  private static void processLogFile(FileInputStream rif) throws IOException {
    Log.OLogEntry entry;
    Log.Entry edit;
    do {
      entry = Log.OLogEntry.parseDelimitedFrom(rif);
      // if ! at EOF                      z
      if (entry != null) {
        edit = Log.Entry.parseFrom(entry.getValue());
        HRegion recoveryRegion = onlineRegions.get(edit.getRegionInfo());

        if (recoveryRegion.getLastFlushTime() >= edit.getTs()) {
          Put put = new Put(edit.getKey().toByteArray());
          put.add(edit.getFamily().toByteArray(),
              edit.getColumn().toByteArray(),
              edit.getTs(),
              edit.getValue().toByteArray());
          put.setDurability(Durability.SKIP_WAL);
          recoveryRegion.put(put);
        }
      }
    } while (entry != null);
  }

  public static HRegion getOnlineRegion(final String encodedRegionName)
      throws RegionNotFoundException {

    if (encodedRegionName.equals("1")) {
      return onlineRegions.regions().iterator().next();
    }

    HRegion region = onlineRegions.get(encodedRegionName);

    if (region == null) {
      String error = "Attempt to get an nonexistent/offline region";
      throw new RegionNotFoundException(error);
    }

    return region;
  }

  private static class RegionNotFoundException extends RuntimeException {
    public RegionNotFoundException(String s) {
      super(s);
    }
  }
}