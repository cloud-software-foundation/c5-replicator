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
import ohmdb.io.encoding.HFileBlockDefaultDecodingContext;
import ohmdb.io.encoding.HFileBlockDefaultEncodingContext;
import ohmdb.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Does not perform any kind of encoding/decoding.
 */
public class NoOpDataBlockEncoder implements HFileDataBlockEncoder {

  public static final NoOpDataBlockEncoder INSTANCE =
      new NoOpDataBlockEncoder();

  /** Cannot be instantiated. Use {@link #INSTANCE} instead. */
  private NoOpDataBlockEncoder() {
  }

  @Override
  public HFileBlock diskToCacheFormat(HFileBlock block, boolean isCompaction) {
    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      throw new IllegalStateException("Unexpected encoded block");
    }
    return block;
  }

  @Override
  public void beforeWriteToDisk(ByteBuffer in,
      boolean includesMemstoreTS,
      HFileBlockEncodingContext encodeCtx, BlockType blockType)
      throws IOException {
    if (!(encodeCtx.getClass().getName().equals(
        HFileBlockDefaultEncodingContext.class.getName()))) {
      throw new IOException (this.getClass().getName() + " only accepts " +
          HFileBlockDefaultEncodingContext.class.getName() + ".");
    }

    HFileBlockDefaultEncodingContext defaultContext =
        (HFileBlockDefaultEncodingContext) encodeCtx;
    defaultContext.compressAfterEncoding(in.array(), blockType);
  }

  @Override
  public boolean useEncodedScanner(boolean isCompaction) {
    return false;
  }

  @Override
  public void saveMetadata(HFile.Writer writer) {
  }

  @Override
  public DataBlockEncoding getEncodingOnDisk() {
    return DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEncodingInCache() {
    return DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return DataBlockEncoding.NONE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public HFileBlockEncodingContext newOnDiskDataBlockEncodingContext(
      Algorithm compressionAlgorithm, byte[] dummyHeader) {
    return new HFileBlockDefaultEncodingContext(compressionAlgorithm,
        null, dummyHeader);
  }

  @Override
  public HFileBlockDecodingContext newOnDiskDataBlockDecodingContext(
      Algorithm compressionAlgorithm) {
    return new HFileBlockDefaultDecodingContext(compressionAlgorithm);
  }

}
