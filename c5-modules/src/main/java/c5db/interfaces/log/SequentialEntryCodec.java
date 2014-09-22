/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.interfaces.log;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static c5db.log.EntryEncodingUtil.CrcError;

/**
 * Encapsulates capability to serialize and deserialize log entry objects.
 *
 * @param <E> Type of the entry to encode/serialize and decode/deserialize
 */
public interface SequentialEntryCodec<E extends SequentialEntry> {
  /**
   * Serialize an entry, including prepending any length necessary to reconstruct
   * the entry, and including any necessary CRCs.
   *
   * @param entry An entry to be serialized.
   * @return An array of ByteBuffer containing the serialized data.
   */
  ByteBuffer[] encode(E entry);

  /**
   * Deserialize an entry from an input stream, and check its CRC.
   *
   * @param inputStream An open input stream, positioned at the start of an entry.
   * @return The reconstructed entry.
   * @throws c5db.log.EntryEncodingUtil.CrcError
   * @throws java.io.IOException
   */
  E decode(InputStream inputStream) throws IOException, CrcError;

  /**
   * Skip over an entry in the input stream, returning the sequence number of the entry encountered.
   *
   * @param inputStream An open input stream, positioned at the start of an entry.
   * @return The sequence number of the entry encountered.
   * @throws c5db.log.EntryEncodingUtil.CrcError
   * @throws java.io.IOException
   */
  long skipEntryAndReturnSeqNum(InputStream inputStream) throws IOException, CrcError;
}
