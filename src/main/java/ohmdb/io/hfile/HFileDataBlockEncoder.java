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

/*
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

import ohmdb.io.encoding.DataBlockEncoding;
import ohmdb.io.encoding.HFileBlockDecodingContext;
import ohmdb.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Controls what kind of data block encoding is used. If data block encoding is
 * not set or the given block is not a data block (encoded or not), methods
 * should just return the unmodified block.
 */
@InterfaceAudience.Private
public interface HFileDataBlockEncoder {
  /** Type of encoding used for data blocks in HFile. Stored in file info. */
  byte[] DATA_BLOCK_ENCODING = Bytes.toBytes("DATA_BLOCK_ENCODING");

  /**
   * Converts a block from the on-disk format to the in-cache format. Called in
   * the following cases:
   * <ul>
   * <li>After an encoded or unencoded data block is read from disk, but before
   * it is put into the cache.</li>
   * <li>To convert brand-new blocks to the in-cache format when doing
   * cache-on-write.</li>
   * </ul>
   * @param block a block in an on-disk format (read from HFile or freshly
   *          generated).
   * @return non null block which is coded according to the settings.
   */
  public HFileBlock diskToCacheFormat(HFileBlock block,
      boolean isCompaction);

  /**
   * Should be called before an encoded or unencoded data block is written to
   * disk.
   * @param in KeyValues next to each other
   * @param encodingResult the encoded result
   * @param blockType block type
   * @throws IOException
   */
  public void beforeWriteToDisk(
      ByteBuffer in, boolean includesMemstoreTS,
      HFileBlockEncodingContext encodingResult,
      BlockType blockType) throws IOException;

  /**
   * Decides whether we should use a scanner over encoded blocks.
   * @param isCompaction whether we are in a compaction.
   * @return Whether to use encoded scanner.
   */
  public boolean useEncodedScanner(boolean isCompaction);

  /**
   * Save metadata in HFile which will be written to disk
   * @param writer writer for a given HFile
   * @exception IOException on disk problems
   */
  public void saveMetadata(HFile.Writer writer)
      throws IOException;

  /** @return the on-disk data block encoding */
  public DataBlockEncoding getEncodingOnDisk();

  /** @return the preferred in-cache data block encoding for normal reads */
  public DataBlockEncoding getEncodingInCache();

  /**
   * @return the effective in-cache data block encoding, taking into account
   *         whether we are doing a compaction.
   */
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction);

  /**
   * Create an encoder specific encoding context object for writing. And the
   * encoding context should also perform compression if compressionAlgorithm is
   * valid.
   *
   * @param compressionAlgorithm compression algorithm
   * @param headerBytes header bytes
   * @return a new {@link HFileBlockEncodingContext} object
   */
  public HFileBlockEncodingContext newOnDiskDataBlockEncodingContext(
      Algorithm compressionAlgorithm, byte[] headerBytes);

  /**
   * create a encoder specific decoding context for reading. And the
   * decoding context should also do decompression if compressionAlgorithm
   * is valid.
   *
   * @param compressionAlgorithm
   * @return a new {@link HFileBlockDecodingContext} object
   */
  public HFileBlockDecodingContext newOnDiskDataBlockDecodingContext(
      Algorithm compressionAlgorithm);

}
