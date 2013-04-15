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

/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package ohmdb.io.hfile;

import ohmdb.io.HeapSize;
import ohmdb.io.hfile.BlockType.BlockCategory;
import ohmdb.io.hfile.bucket.BucketCache;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * CombinedBlockCache is an abstraction layer that combines
 * {@link LruBlockCache} and {@link BucketCache}. The smaller lruCache is used
 * to cache bloom blocks and index blocks , the larger bucketCache is used to
 * cache data blocks. getBlock reads first from the smaller lruCache before
 * looking for the block in the bucketCache. Metrics are the combined size and
 * hits and misses of both caches.
 *
 **/
public class CombinedBlockCache implements BlockCache, HeapSize {

  private final LruBlockCache lruCache;
  private final BucketCache bucketCache;
  private final CombinedCacheStats combinedCacheStats;

  public CombinedBlockCache(LruBlockCache lruCache, BucketCache bucketCache) {
    this.lruCache = lruCache;
    this.bucketCache = bucketCache;
    this.combinedCacheStats = new CombinedCacheStats(lruCache.getStats(),
        bucketCache.getStats());
  }

  @Override
  public long heapSize() {
    return lruCache.heapSize() + bucketCache.heapSize();
  }


  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    boolean isMetaBlock = buf.getBlockType().getCategory() != BlockCategory.DATA;
    if (isMetaBlock) {
      lruCache.cacheBlock(cacheKey, buf, inMemory);
    } else {
      bucketCache.cacheBlock(cacheKey, buf, inMemory);
    }
  }


  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
      boolean repeat) {
    if (lruCache.containsBlock(cacheKey)) {
      return lruCache.getBlock(cacheKey, caching, repeat);
    }
    return bucketCache.getBlock(cacheKey, caching, repeat);

  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    return lruCache.evictBlock(cacheKey) || bucketCache.evictBlock(cacheKey);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    return lruCache.evictBlocksByHfileName(hfileName)
        + bucketCache.evictBlocksByHfileName(hfileName);
  }

  @Override
  public CacheStats getStats() {
    return this.combinedCacheStats;
  }

  @Override
  public void shutdown() {
    lruCache.shutdown();
    bucketCache.shutdown();

  }

  @Override
  public long size() {
    return lruCache.size() + bucketCache.size();
  }

  @Override
  public long getFreeSize() {
    return lruCache.getFreeSize() + bucketCache.getFreeSize();
  }

  @Override
  public long getCurrentSize() {
    return lruCache.getCurrentSize() + bucketCache.getCurrentSize();
  }

  @Override
  public long getEvictedCount() {
    return lruCache.getEvictedCount() + bucketCache.getEvictedCount();
  }

  @Override
  public long getBlockCount() {
    return lruCache.getBlockCount() + bucketCache.getBlockCount();
  }

  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(
      Configuration conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  private static class CombinedCacheStats extends CacheStats {
    private final CacheStats lruCacheStats;
    private final CacheStats bucketCacheStats;

    CombinedCacheStats(CacheStats lbcStats, CacheStats fcStats) {
      this.lruCacheStats = lbcStats;
      this.bucketCacheStats = fcStats;
    }

    @Override
    public long getRequestCount() {
      return lruCacheStats.getRequestCount()
          + bucketCacheStats.getRequestCount();
    }

    @Override
    public long getRequestCachingCount() {
      return lruCacheStats.getRequestCachingCount()
          + bucketCacheStats.getRequestCachingCount();
    }

    @Override
    public long getMissCount() {
      return lruCacheStats.getMissCount() + bucketCacheStats.getMissCount();
    }

    @Override
    public long getMissCachingCount() {
      return lruCacheStats.getMissCachingCount()
          + bucketCacheStats.getMissCachingCount();
    }

    @Override
    public long getHitCount() {
      return lruCacheStats.getHitCount() + bucketCacheStats.getHitCount();
    }

    @Override
    public long getHitCachingCount() {
      return lruCacheStats.getHitCachingCount()
          + bucketCacheStats.getHitCachingCount();
    }

    @Override
    public long getEvictionCount() {
      return lruCacheStats.getEvictionCount()
          + bucketCacheStats.getEvictionCount();
    }

    @Override
    public long getEvictedCount() {
      return lruCacheStats.getEvictedCount()
          + bucketCacheStats.getEvictedCount();
    }

    @Override
    public double getHitRatioPastNPeriods() {
      double ratio = ((double) (lruCacheStats.getSumHitCountsPastNPeriods() + bucketCacheStats
          .getSumHitCountsPastNPeriods()) / (double) (lruCacheStats
          .getSumRequestCountsPastNPeriods() + bucketCacheStats
          .getSumRequestCountsPastNPeriods()));
      return Double.isNaN(ratio) ? 0 : ratio;
    }

    @Override
    public double getHitCachingRatioPastNPeriods() {
      double ratio = ((double) (lruCacheStats
          .getSumHitCachingCountsPastNPeriods() + bucketCacheStats
          .getSumHitCachingCountsPastNPeriods()) / (double) (lruCacheStats
          .getSumRequestCachingCountsPastNPeriods() + bucketCacheStats
          .getSumRequestCachingCountsPastNPeriods()));
      return Double.isNaN(ratio) ? 0 : ratio;
    }

  }

}
