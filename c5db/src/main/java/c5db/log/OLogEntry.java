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
import c5db.replication.generated.LogEntry;
import io.protostuff.Schema;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.EntryEncodingUtil.appendCrcToBufferList;
import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static c5db.log.EntryEncodingUtil.getAndCheckContent;
import static c5db.log.EntryEncodingUtil.skip;

public class OLogEntry implements SequentialEntry {
  private final long seqNum;
  private final long electionTerm;
  private final List<ByteBuffer> buffers;

  public OLogEntry(long seqNum, long electionTerm, List<ByteBuffer> buffers) {
    assert buffers != null;

    this.seqNum = seqNum;
    this.electionTerm = electionTerm;
    this.buffers = sliceAll(buffers);
  }

  @Override
  public long getSeqNum() {
    return seqNum;
  }

  @Override
  public long getElectionTerm() {
    return electionTerm;
  }

  public List<ByteBuffer> getBuffers() {
    return buffers;
  }

  public int contentLength() {
    return EntryEncodingUtil.sumLengths(buffers);
  }

  public LogEntry toProtostuffMessage() {
    return new LogEntry(electionTerm, seqNum, buffers);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || (o.getClass() != this.getClass())) {
      return false;
    }
    OLogEntry that = (OLogEntry) o;
    return this.electionTerm == that.getElectionTerm()
        && this.seqNum == that.getSeqNum()
        && this.buffers.equals(that.getBuffers());
  }

  @Override
  public int hashCode() {
    return ((int) seqNum) * 31 +
        ((int) electionTerm) * 31 +
        buffers.hashCode();
  }

  @Override
  public String toString() {
    return "OLogEntry{" +
        "seqNum='" + seqNum + '\'' +
        ", electionTerm=" + electionTerm +
        ", buffers=" + buffers;
  }

  private static List<ByteBuffer> sliceAll(List<ByteBuffer> buffers) {
    return Lists.transform(buffers, ByteBuffer::slice);
  }

  public static class Codec implements EncodedSequentialLog.Codec<OLogEntry> {
    private static final Schema<OLogEntryHeader> SCHEMA = OLogEntryHeader.getSchema();
    private static int CRC_BYTES = 4;

    @Override
    public ByteBuffer[] encode(OLogEntry entry) {
      try {
        final OLogEntryHeader header = createHeader(entry);
        final List<ByteBuffer> entryBufs = encodeWithLengthAndCrc(SCHEMA, header);

        if (header.getContentLength() > 0) {
          entryBufs.addAll(appendCrcToBufferList(entry.getBuffers()));
        }

        return Iterables.toArray(entryBufs, ByteBuffer.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public OLogEntry decode(InputStream inputStream) throws IOException, CrcError {
      final OLogEntryHeader header = decodeAndCheckCrc(inputStream, SCHEMA);
      final ByteBuffer buffer = getAndCheckContent(inputStream, header.getContentLength());
      return new OLogEntry(
          header.getSeqNum(),
          header.getTerm(),
          Lists.newArrayList(buffer));
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

    private static OLogEntryHeader createHeader(OLogEntry entry) {
      return new OLogEntryHeader(
          entry.getSeqNum(),
          entry.getElectionTerm(),
          entry.contentLength());
    }
  }
}
