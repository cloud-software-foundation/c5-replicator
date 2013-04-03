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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * IO engine that stores data to a file on the local file system.
 */
@InterfaceAudience.Private
public class FileIOEngine implements IOEngine {
  static final Log LOG = LogFactory.getLog(FileIOEngine.class);

  private FileChannel fileChannel = null;

  public FileIOEngine(String filePath, long fileSize) throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(filePath, "rw");
      raf.setLength(fileSize);
      fileChannel = raf.getChannel();
      LOG.info("Allocating " + StringUtils.byteDesc(fileSize)
          + ", on the path:" + filePath);
    } catch (java.io.FileNotFoundException fex) {
      LOG.error("Can't create bucket cache file " + filePath, fex);
      throw fex;
    } catch (IOException ioex) {
      LOG.error("Can't extend bucket cache file; insufficient space for "
          + StringUtils.byteDesc(fileSize), ioex);
      if (raf != null) raf.close();
      throw ioex;
    }
  }

  /**
   * File IO engine is always able to support persistent storage for the cache
   * @return true
   */
  @Override
  public boolean isPersistent() {
    return true;
  }

  /**
   * Transfers data from file to the given byte buffer
   * @param dstBuffer the given byte buffer into which bytes are to be written
   * @param offset The offset in the file where the first byte to be read
   * @throws IOException
   */
  @Override
  public void read(ByteBuffer dstBuffer, long offset) throws IOException {
    fileChannel.read(dstBuffer, offset);
  }

  /**
   * Transfers data from the given byte buffer to file
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the file where the first byte to be written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    fileChannel.write(srcBuffer, offset);
  }

  /**
   * Sync the data to file after writing
   * @throws IOException
   */
  @Override
  public void sync() throws IOException {
    fileChannel.force(true);
  }

  /**
   * Close the file
   */
  @Override
  public void shutdown() {
    try {
      fileChannel.close();
    } catch (IOException ex) {
      LOG.error("Can't shutdown cleanly", ex);
    }
  }
}
