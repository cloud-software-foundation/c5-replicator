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
package ohmdb.io.hfile.bucket;

import ohmdb.io.hfile.CacheStats;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class that implements cache metrics for bucket cache.
 */
@InterfaceAudience.Private
public class BucketCacheStats extends CacheStats {
  private final AtomicLong ioHitCount = new AtomicLong(0);
  private final AtomicLong ioHitTime = new AtomicLong(0);
  private final static int nanoTime = 1000000;
  private long lastLogTime = EnvironmentEdgeManager.currentTimeMillis();

  public void ioHit(long time) {
    ioHitCount.incrementAndGet();
    ioHitTime.addAndGet(time);
  }

  public long getIOHitsPerSecond() {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long took = (now - lastLogTime) / 1000;
    lastLogTime = now;
    return ioHitCount.get() / took;
  }

  public double getIOTimePerHit() {
    long time = ioHitTime.get() / nanoTime;
    long count = ioHitCount.get();
    return ((float) time / (float) count);
  }

  public void reset() {
    ioHitCount.set(0);
    ioHitTime.set(0);
  }
}
