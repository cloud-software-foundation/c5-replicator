package ohmdb;

import org.apache.hadoop.hbase.regionserver.HRegion;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum OnlineRegions {
  INSTANCE;
  private final Map<String, HRegion> onlineRegions =
      new ConcurrentHashMap<>();

  public void put( HRegion region){
    this.onlineRegions.put(region.getRegionNameAsString(), region);
  }

  public Collection<HRegion> regions(){
    return this.onlineRegions.values();
  }

  public HRegion get(String regionInfo) {
    return this.onlineRegions.get(regionInfo);
  }
}
