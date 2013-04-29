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
package ohmdb.io.encoding;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;

import java.nio.ByteBuffer;

/**
 * Stores the state of data block encoder at the beginning of new key.
 */
@InterfaceAudience.Private
class CompressionState {
  int keyLength;
  int valueLength;

  short rowLength;
  int prevOffset = FIRST_KEY;
  byte familyLength;
  int qualifierLength;
  byte type;

  private final static int FIRST_KEY = -1;

  boolean isFirst() {
    return prevOffset == FIRST_KEY;
  }

  /**
   * Analyze the key and fill the state.
   * Uses mark() and reset() in ByteBuffer.
   * @param in Buffer at the position where key starts
   * @param keyLength Length of key in bytes
   * @param valueLength Length of values in bytes
   */
  void readKey(ByteBuffer in, int keyLength, int valueLength) {
    readKey(in, keyLength, valueLength, 0, null);
  }

  /**
   * Analyze the key and fill the state assuming we know previous state.
   * Uses mark() and reset() in ByteBuffer to avoid moving the position.
   * <p>
   * This method overrides all the fields of this instance, except
   * {@link #prevOffset}, which is usually manipulated directly by encoders
   * and decoders.
   * @param in Buffer at the position where key starts
   * @param keyLength Length of key in bytes
   * @param valueLength Length of values in bytes
   * @param commonPrefix how many first bytes are common with previous KeyValue
   * @param previousState State from previous KeyValue
   */
  void readKey(ByteBuffer in, int keyLength, int valueLength,
      int commonPrefix, CompressionState previousState) {
    this.keyLength = keyLength;
    this.valueLength = valueLength;

    // fill the state
    in.mark(); // mark beginning of key

    if (commonPrefix < KeyValue.ROW_LENGTH_SIZE) {
      rowLength = in.getShort();
      ByteBufferUtils.skip(in, rowLength);

      familyLength = in.get();

      qualifierLength = keyLength - rowLength - familyLength -
          KeyValue.KEY_INFRASTRUCTURE_SIZE;
      ByteBufferUtils.skip(in, familyLength + qualifierLength);
    } else {
      rowLength = previousState.rowLength;
      familyLength = previousState.familyLength;
      qualifierLength = previousState.qualifierLength +
          keyLength - previousState.keyLength;
      ByteBufferUtils.skip(in, (KeyValue.ROW_LENGTH_SIZE +
          KeyValue.FAMILY_LENGTH_SIZE) +
          rowLength + familyLength + qualifierLength);
    }

    readTimestamp(in);

    type = in.get();

    in.reset();
  }

  protected void readTimestamp(ByteBuffer in) {
    // used in subclasses to add timestamp to state
    ByteBufferUtils.skip(in, KeyValue.TIMESTAMP_SIZE);
  }

  void copyFrom(CompressionState state) {
    keyLength = state.keyLength;
    valueLength = state.valueLength;

    rowLength = state.rowLength;
    prevOffset = state.prevOffset;
    familyLength = state.familyLength;
    qualifierLength = state.qualifierLength;
    type = state.type;
  }
}
