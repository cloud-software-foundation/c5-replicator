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

import c5db.generated.OLogEntryHeader;
import com.google.common.math.IntMath;
import io.protostuff.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.getAndCheckContent;
import static c5db.log.EntryEncodingUtil.skip;

public final class OLogEntryDescription extends SequentialEntry {
  private final long electionTerm;
  private final int contentLength;
  private final boolean headerCrcIsValid;
  private final boolean contentCrcIsValid;

  public OLogEntryDescription(long seqNum,
                              long electionTerm,
                              int contentLength,
                              boolean headerCrcIsValid,
                              boolean contentCrcIsValid) {
    super(seqNum);

    this.electionTerm = electionTerm;
    this.contentLength = contentLength;
    this.headerCrcIsValid = headerCrcIsValid;
    this.contentCrcIsValid = contentCrcIsValid;
  }

  @SuppressWarnings("UnusedDeclaration")
  public long getElectionTerm() {
    return electionTerm;
  }

  @SuppressWarnings("UnusedDeclaration")
  public int contentLength() {
    return contentLength;
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

    return seqNum == that.seqNum
        && electionTerm == that.electionTerm
        && contentLength == that.contentLength
        && contentCrcIsValid == that.contentCrcIsValid
        && headerCrcIsValid == that.headerCrcIsValid;
  }

  @Override
  public int hashCode() {
    int result = (int) (seqNum ^ (seqNum >>> 32));
    result = 31 * result + (int) (electionTerm ^ (electionTerm >>> 32));
    result = 31 * result + contentLength;
    result = 31 * result + (headerCrcIsValid ? 1 : 0);
    result = 31 * result + (contentCrcIsValid ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OLogEntryDescription{" +
        "seqNum='" + seqNum + '\'' +
        ", electionTerm=" + electionTerm +
        ", contentLength=" + contentLength +
        ", headerCrcIsValid=" + headerCrcIsValid +
        ", contentCrcIsValid=" + contentCrcIsValid;
  }

  public static class Codec implements EncodedSequentialLog.Codec<OLogEntryDescription> {
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
      try {
        getAndCheckContent(inputStream, header.getContentLength());
      } catch (CrcError e) {
        contentCrcIsValid = false;
      }

      return new OLogEntryDescription(
          header.getSeqNum(),
          header.getTerm(),
          header.getContentLength(),
          true,
          contentCrcIsValid);
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
  }
}
