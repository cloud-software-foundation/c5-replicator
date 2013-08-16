package ohmdb.regionserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.jpountz.lz4.LZ4BlockInputStream;
import ohmdb.client.OhmConstants;
import ohmdb.generated.Log;
import ohmdb.log.OLog;
import ohmdb.log.OLogShim;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class OhmServer implements Runnable {
  private final int port;
  protected final static Map<String, HRegion> onlineRegions =
      new ConcurrentHashMap<>();

  public OhmServer(final int port) {
    this.port = port;
  }

  @Override
  public void run() {
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new OhmServerInitializer());

      ChannelFuture f = b.bind(port).sync();
      f.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  private static String getRandomPath() {
    Random random = new Random();
    return OhmConstants.TMP_DIR + random.nextInt();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    Path path;
    if (args.length < 2) {
      path = new Path(getRandomPath());
    } else {
      path = new Path(args[1]);
    }

    RegistryFile registryFile = new RegistryFile(path.toString());
    OLog.moveAwayOldLogs(path.toString());

    OLogShim hLog;
    if (existingRegister(registryFile)) {
      hLog = recoverOhmServer(conf, path, registryFile);
    } else {
      hLog = bootStrapRegions(conf, path, registryFile);
    }

    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = OhmConstants.DEFAULT_PORT;
    }

    OhmServer ohmServer = new OhmServer(port);
    Thread t = new Thread(ohmServer);
    t.start();

    for (int i = 0; t.isAlive(); i++) {
      Thread.sleep(OhmConstants.FLUSH_PERIOD);
      flushAndCompact(hLog, i);
    }
  }

  private static void flushAndCompact(OLogShim hLog, int i) throws IOException {
    for (HRegion region : onlineRegions.values()) {
      region.flushcache();
      hLog.rollWriter();
      if (i % OhmConstants.AMOUNT_OF_FLUSH_PER_COMPACT == 0) {
        region.compactStores();
      }
      if (i % OhmConstants.AMOUNT_OF_FLUSH_PER_OLD_LOG_CLEAR == 0) {
        hLog.clearOldLogs(System.currentTimeMillis()
            - OhmConstants.OLD_LOG_CLEAR_AGE);
      }
    }
  }

  private static OLogShim recoverOhmServer(Configuration conf,
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

      HRegion region = HRegion.openHRegion(conf,
          path.getFileSystem(conf),
          path,
          regionInfo,
          hTableDescriptor,
          hLog);
      onlineRegions.put(region.getRegionNameAsString(), region);
    }

    logReplay(path);
    hLog.clearOldLogs(0);
    return hLog;
  }

  private static OLogShim bootStrapRegions(Configuration conf,
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
        path,
        conf,
        hTableDescriptor,
        hlog);
    registryFile.addEntry(hRegionInfo, new HColumnDescriptor("cf"));
    onlineRegions.put(region.getRegionNameAsString(), region);
    return hlog;
  }

  private static boolean existingRegister(RegistryFile registryFile)
      throws IOException {
    return registryFile.getRegistry().size() != 0;
  }

  private static void logReplay(final Path path) throws IOException {
    java.nio.file.Path archiveLogPath = Paths.get(path.toString(),
        OhmConstants.ARCHIVE_DIR);
    File[] files = archiveLogPath.toFile().listFiles();

    if (files == null) {
      return;
    }

    for (File file : files) {
      FileInputStream rif = new FileInputStream(file);
      Log.Entry edit;
      do {
        edit = Log.Entry.parseDelimitedFrom(rif);
        if (edit != null) {
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
      } while (edit != null);
      for (HRegion r : onlineRegions.values()) {
        r.flushcache();
      }
    }
    for (HRegion r : onlineRegions.values()) {
      r.compactStores();
    }

    for (HRegion r : onlineRegions.values()) {
      r.waitForFlushesAndCompactions();
    }

    //TODO WE SHOULDN"T BE ONLINE TIL THIS HAPPENS
  }

  public static HRegion getOnlineRegion(final String encodedRegionName)
      throws RegionNotFoundException {

    if (encodedRegionName.equals("1")) {
      return onlineRegions.values().iterator().next();
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

