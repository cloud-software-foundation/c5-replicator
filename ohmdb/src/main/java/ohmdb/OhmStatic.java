package ohmdb;

import ohmdb.client.OhmConstants;
import ohmdb.generated.Log;
import ohmdb.regionserver.RegistryFile;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class OhmStatic {
    private final static OnlineRegions onlineRegions = OnlineRegions.INSTANCE;

    public static String getRandomPath() {
        Random random = new Random();
        return OhmConstants.TMP_DIR + random.nextInt();
    }


    public static boolean existingRegister(RegistryFile registryFile)
            throws IOException {
        return registryFile.getRegistry().regions.size() != 0;
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