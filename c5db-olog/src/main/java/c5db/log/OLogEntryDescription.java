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

package c5db.log;

import c5db.generated.OLogContentType;
import c5db.generated.OLogEntryHeader;
import c5db.interfaces.log.SequentialEntry;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.interfaces.replication.QuorumConfiguration;
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.math.IntMath;
import io.protostuff.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.getAndCheckContent;
import static c5db.log.EntryEncodingUtil.skip;

/**
 * A description of an OLogEntry for use by reader utilities.
 * <p>
 * OLogEntry log entries encoded to some medium, such as a log file on disk, using
 * {@link c5db.log.OLogEntry.Codec} may be decoded (deserialized) using a different Codec.
 * This class and its Codec are one such example. Their goal is to look at a serialized
 * OLogEntry and describe its features. The intended use of this is for readers such
 * as CatOLog that may wish to analyze and display features of the serialized entry
 * not normally visible when decoded as an OLogEntry object, such as CRC check validity.
 * <p>
 * As such, the provided Codec below can only decode, not encode.
 */
public final class OLogEntryDescription extends SequentialEntry {
  private final long electionTerm;
  private final int contentLength;
  private final OLogContentType type;
  private final boolean headerCrcIsValid;
  private final boolean contentCrcIsValid;

  // Only used for type == QUORUM_CONFIGURATION
  private final QuorumConfiguration quorumConfiguration;

  public OLogEntryDescription(long seqNum,
                              long electionTerm,
                              int contentLength,
                              OLogContentType type,
                              boolean headerCrcIsValid,
                              boolean contentCrcIsValid,
                              QuorumConfiguration quorumConfiguration) {
    super(seqNum);

    this.electionTerm = electionTerm;
    this.contentLength = contentLength;
    this.type = type;
    this.headerCrcIsValid = headerCrcIsValid;
    this.contentCrcIsValid = contentCrcIsValid;
    this.quorumConfiguration = quorumConfiguration;
  }

  public long getElectionTerm() {
    return electionTerm;
  }

  public int getContentLength() {
    return contentLength;
  }

  public OLogContentType getType() {
    return type;
  }

  public boolean isHeaderCrcValid() {
    return headerCrcIsValid;
  }

  public boolean isContentCrcValid() {
    return contentCrcIsValid;
  }

  public QuorumConfiguration getQuorumConfiguration() {
    return quorumConfiguration;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OLogEntryDescription that = (OLogEntryDescription) o;

    return seqNum == that.getSeqNum()
        && electionTerm == that.electionTerm
        && contentLength == that.contentLength
        && type == that.type
        && contentCrcIsValid == that.contentCrcIsValid
        && headerCrcIsValid == that.headerCrcIsValid
        && (quorumConfiguration == that.quorumConfiguration
        || quorumConfiguration.equals(that.quorumConfiguration));
  }

  @Override
  public int hashCode() {
    int result = (int) (seqNum ^ (seqNum >>> 32));
    result = 31 * result + (int) (electionTerm ^ (electionTerm >>> 32));
    result = 31 * result + contentLength;
    result = 31 * result + type.hashCode();
    result = 31 * result + (headerCrcIsValid ? 1 : 0);
    result = 31 * result + (contentCrcIsValid ? 1 : 0);
    result = 31 * result + quorumConfiguration.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OLogEntryDescription{" +
        "seqNum='" + seqNum + '\'' +
        ", electionTerm=" + electionTerm +
        ", contentLength=" + contentLength +
        ", type=" + type +
        ", headerCrcIsValid=" + headerCrcIsValid +
        ", contentCrcIsValid=" + contentCrcIsValid +
        ", quorumConfiguration=" + quorumConfiguration;
  }

  public static class Codec implements SequentialEntryCodec<OLogEntryDescription> {
    private static final Schema<OLogEntryHeader> SCHEMA = OLogEntryHeader.getSchema();
    private static final int CRC_BYTES = 4;

    @Override
    public ByteBuffer[] encode(OLogEntryDescription entry) {
      // TODO since this is essentially a read-only codec, it doesn't need to encode. Does this mean the Codec
      // TODO interface/concept is the wrong abstraction, or should be broken down further?
      throw new UnsupportedOperationException("encode");
    }

    @Override
    public OLogEntryDescription decode(InputStream inputStream) throws IOException, CrcError {
      // TODO (possibly) handle even a corrupted header
      final OLogEntryHeader header = decodeAndCheckCrc(inputStream, SCHEMA);

      boolean contentCrcIsValid = true;
      ByteBuffer contentBuffer = null;
      QuorumConfiguration quorumConfiguration = null;

      try {
        contentBuffer = getAndCheckContent(inputStream, header.getContentLength());
      } catch (CrcError e) {
        contentCrcIsValid = false;
      }

      if (contentBuffer != null && header.getType() == OLogContentType.QUORUM_CONFIGURATION) {
        quorumConfiguration = deserializeQuorumConfiguration(header, contentBuffer);
      }

      return new OLogEntryDescription(
          header.getSeqNum(),
          header.getTerm(),
          header.getContentLength(),
          header.getType(),
          true,
          contentCrcIsValid,
          quorumConfiguration);
    }

    @Override
    public long skipEntryAndReturnSeqNum(InputStream inputStream) throws IOException {
      final OLogEntryHeader header = decodeAndCheckCrc(inputStream, SCHEMA);
      skipContent(inputStream, header.getContentLength());
      return header.getSeqNum();
    }

    private void skipContent(InputStream inputStream, int contentLength) throws IOException {
      skip(inputStream, IntMath.checkedAdd(contentLength, CRC_BYTES));
    }

    private QuorumConfiguration deserializeQuorumConfiguration(OLogEntryHeader header, ByteBuffer buffer) {
      assert header.getType() == OLogContentType.QUORUM_CONFIGURATION;

      OLogContent content = OLogContent.deserialize(buffer, OLogContentType.QUORUM_CONFIGURATION);
      OLogEntry entry = new OLogEntry(0, 0, content);
      QuorumConfigurationMessage quorumConfigurationMessage = entry.toProtostuff().getQuorumConfiguration();

      return QuorumConfiguration.fromProtostuff(quorumConfigurationMessage);
    }
  }
}
