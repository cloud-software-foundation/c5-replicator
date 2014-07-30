/*
 * Copyright (C) 2014  Ohm Data
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
