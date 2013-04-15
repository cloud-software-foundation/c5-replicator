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

import org.apache.hadoop.hbase.util.ByteBufferArray;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * IO engine that stores data on the memory using an array of ByteBuffers
 * {@link ByteBufferArray}
 */
public class ByteBufferIOEngine implements IOEngine {

  private ByteBufferArray bufferArray;

  /**
   * Construct the ByteBufferIOEngine with the given capacity
   * @param capacity
   * @param direct true if allocate direct buffer
   * @throws IOException
   */
  public ByteBufferIOEngine(long capacity, boolean direct)
      throws IOException {
    bufferArray = new ByteBufferArray(capacity, direct);
  }

  /**
   * Memory IO engine is always unable to support persistent storage for the
   * cache
   * @return false
   */
  @Override
  public boolean isPersistent() {
    return false;
  }

  /**
   * Transfers data from the buffer array to the given byte buffer
   * @param dstBuffer the given byte buffer into which bytes are to be written
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          read
   * @throws IOException
   */
  @Override
  public void read(ByteBuffer dstBuffer, long offset) throws IOException {
    assert dstBuffer.hasArray();
    bufferArray.getMultiple(offset, dstBuffer.remaining(), dstBuffer.array(),
        dstBuffer.arrayOffset());
  }

  /**
   * Transfers data from the given byte buffer to the buffer array
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    assert srcBuffer.hasArray();
    bufferArray.putMultiple(offset, srcBuffer.remaining(), srcBuffer.array(),
        srcBuffer.arrayOffset());
  }

  /**
   * No operation for the sync in the memory IO engine
   */
  @Override
  public void sync() {

  }

  /**
   * No operation for the shutdown in the memory IO engine
   */
  @Override
  public void shutdown() {

  }
}
