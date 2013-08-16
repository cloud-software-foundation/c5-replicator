package ohmdb.regionserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;

import java.io.IOException;
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

  public static void main(String[] args) throws IOException {
    String tableName = "tableName";
    byte[] startKey = {0};
    byte[] endKey = {};
    HRegionInfo hRegionInfo = new HRegionInfo(Bytes.toBytes(tableName),
        startKey,
        endKey);
    Configuration conf = HBaseConfiguration.create();
    Random random = new Random();
    Path path;
    if (args.length < 2) {
      path = new Path("/tmp/" + random.nextInt());
    } else {
      path = new Path(args[1]);
    }

    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
    hTableDescriptor.addFamily(new HColumnDescriptor("cf"));
    HRegion region = null;
    try {
      if (args.length < 2 && path.getFileSystem(conf).exists(path)) {
        MetaUtils metaUtils = new MetaUtils();
        HLog hlog = metaUtils.getLog();
        region = HRegion.openHRegion(conf,
            path.getFileSystem(conf),
            path,
            hRegionInfo,
            hTableDescriptor,
            hlog);
      } else {
        region = HRegion.createHRegion(hRegionInfo,
            path,
            conf,
            hTableDescriptor);
      }
      onlineRegions.put("1", region);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 8080;
    }
    new OhmServer(port).run();
    if (region != null) {
      region.compactStores();
      HRegion.closeHRegion(region);
    }
  }

  public static HRegion getOnlineRegion(final String encodedRegionName)
      throws RegionNotFoundException {
    HRegion region = onlineRegions.get("1");
    if (region == null) {
      throw new RegionNotFoundException("Attempt to get an online region");
    }
    return region;
  }


  private static class RegionNotFoundException extends RuntimeException {
    public RegionNotFoundException(String s) {
      super(s);
    }
  }
}

