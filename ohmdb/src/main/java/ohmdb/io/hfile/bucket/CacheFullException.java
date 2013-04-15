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


import java.io.IOException;

/**
 * Thrown by {@link BucketAllocator#allocateBlock(int)} when cache is full for
 * the requested size
 */
public class CacheFullException extends IOException {
  private static final long serialVersionUID = 3265127301824638920L;
  private int requestedSize, bucketIndex;

  CacheFullException(int requestedSize, int bucketIndex) {
    super();
    this.requestedSize = requestedSize;
    this.bucketIndex = bucketIndex;
  }

  public int bucketIndex() {
    return bucketIndex;
  }

  public int requestedSize() {
    return requestedSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("Allocator requested size ").append(requestedSize);
    sb.append(" for bucket ").append(bucketIndex);
    return sb.toString();
  }
}
