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

package c5db.log;

import c5db.interfaces.log.SequentialEntryCodec;
import c5db.log.generated.OLogHeader;
import com.google.common.collect.Iterables;
import com.google.common.io.CountingInputStream;
import io.protostuff.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;

import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static c5db.log.LogPersistenceService.BytePersistence;
import static c5db.log.LogPersistenceService.PersistenceNavigator;
import static c5db.log.LogPersistenceService.PersistenceNavigatorFactory;
import static c5db.log.LogPersistenceService.PersistenceReader;

/**
 * A SequentialLog of OLogEntry, together with an OLogHeader message. Together, these two
 * objects represent the byte contents present on a single BytePersistence encoded by a
 * QuorumDelegatingLog.
 */
class SequentialLogWithHeader {
  private static final Schema<OLogHeader> HEADER_SCHEMA = OLogHeader.getSchema();
  private static final SequentialEntryCodec<OLogEntry> CODEC = new OLogEntry.Codec();

  public final SequentialLog<OLogEntry> log;
  public final OLogHeader header;

  /**
   * Private constructor; use one of the public static factory methods below.
   */
  private SequentialLogWithHeader(SequentialLog<OLogEntry> log, OLogHeader header) {
    this.log = log;
    this.header = header;
  }

  /**
   * Create a new instance by reading in data from a preexisting BytePersistence. It
   * reads the header and checks its CRC, then creates a SequentialLog to represent
   * the entries.
   *
   * @param persistence      A BytePersistence representing an existing log (at least an
   *                         OLogHeader and zero or more OLogEntry)
   * @param navigatorFactory Used to create a PersistenceNavigator required for the log
   * @return A new SequentialLogWithHeader instance
   * @throws java.io.IOException
   */
  public static SequentialLogWithHeader readLogFromPersistence(BytePersistence persistence,
                                                               PersistenceNavigatorFactory navigatorFactory)
      throws IOException {

    HeaderWithSize headerWithSize = readHeaderFromPersistence(persistence);
    return create(persistence, navigatorFactory, headerWithSize);
  }

  /**
   * Create a new log and header and write them to a new persistence. The header corresponds to the
   * current position and state of the current log (if there is one).
   *
   * @param persistenceService The LogPersistenceService, needed to append the new persistence to
   * @param navigatorFactory   Factory to create the PersistenceNavigator required for the log
   * @param header             OLogHeader to write immediately at the start of the persistence
   * @param quorumId           Quorum ID, needed to determine where to append the persistence
   * @param <P>                The type of the BytePersistence to create and write
   * @throws java.io.IOException
   */
  public static <P extends BytePersistence> SequentialLogWithHeader writeNewLog(
      LogPersistenceService<P> persistenceService, PersistenceNavigatorFactory navigatorFactory,
      OLogHeader header, String quorumId) throws IOException {

    final P persistence = persistenceService.create(quorumId);
    HeaderWithSize headerWithSize = writeHeaderToPersistence(persistence, header);
    persistenceService.append(quorumId, persistence);

    return create(persistence, navigatorFactory, headerWithSize);
  }

  /**
   * Create a PersistenceNavigator for data resident on an existing persistence.
   *
   * @param persistence      A BytePersistence representing an existing log (at least an
   *                         OLogHeader and zero or more entries)
   * @param navigatorFactory Factory to create the PersistenceNavigator with
   * @param entryCodec       Codec to use to decode entries on the persistence
   * @return A new PersistenceNavigator, ready to use with the log
   * @throws IOException
   */
  public static PersistenceNavigator createNavigatorFromPersistence(BytePersistence persistence,
                                                                    PersistenceNavigatorFactory navigatorFactory,
                                                                    SequentialEntryCodec<?> entryCodec)
      throws IOException {

    HeaderWithSize headerWithSize = readHeaderFromPersistence(persistence);

    return createNavigatorForHeader(persistence, navigatorFactory, entryCodec, headerWithSize);
  }


  private static PersistenceNavigator createNavigatorForHeader(BytePersistence persistence,
                                                               PersistenceNavigatorFactory navigatorFactory,
                                                               SequentialEntryCodec<?> entryCodec,
                                                               HeaderWithSize headerWithSize)
      throws IOException {

    final PersistenceNavigator navigator = navigatorFactory.create(persistence, entryCodec, headerWithSize.size);
    navigator.addToIndex(headerWithSize.header.getBaseSeqNum() + 1, headerWithSize.size);

    return navigator;
  }

  private static SequentialLogWithHeader create(BytePersistence persistence,
                                                PersistenceNavigatorFactory navigatorFactory,
                                                HeaderWithSize headerWithSize)
      throws IOException {

    final PersistenceNavigator navigator =
        createNavigatorForHeader(persistence, navigatorFactory, CODEC, headerWithSize);
    final SequentialLog<OLogEntry> log = new EncodedSequentialLog<>(persistence, CODEC, navigator);

    return new SequentialLogWithHeader(log, headerWithSize.header);
  }

  private static HeaderWithSize readHeaderFromPersistence(BytePersistence persistence) throws IOException {

    try (CountingInputStream input = getCountingInputStream(persistence.getReader())) {
      final OLogHeader header = decodeAndCheckCrc(input, HEADER_SCHEMA);
      final long headerSize = input.getCount();

      return new HeaderWithSize(header, headerSize);
    }
  }

  private static HeaderWithSize writeHeaderToPersistence(BytePersistence persistence, OLogHeader header)
      throws IOException {

    final List<ByteBuffer> serializedHeader = encodeWithLengthAndCrc(HEADER_SCHEMA, header);
    persistence.append(Iterables.toArray(serializedHeader, ByteBuffer.class));

    return new HeaderWithSize(header, persistence.size());
  }

  private static CountingInputStream getCountingInputStream(PersistenceReader reader) {
    return new CountingInputStream(Channels.newInputStream(reader));
  }

  private static class HeaderWithSize {
    public final OLogHeader header;
    public final long size;

    private HeaderWithSize(OLogHeader header, long size) {
      this.header = header;
      this.size = size;
    }
  }
}
