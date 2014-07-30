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
import c5db.replication.generated.LogEntry;
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.collect.Iterables;
import com.google.common.math.IntMath;
import io.protostuff.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static c5db.log.EntryEncodingUtil.CrcError;
import static c5db.log.EntryEncodingUtil.appendCrcToBufferList;
import static c5db.log.EntryEncodingUtil.decodeAndCheckCrc;
import static c5db.log.EntryEncodingUtil.encodeWithLengthAndCrc;
import static c5db.log.EntryEncodingUtil.getAndCheckContent;
import static c5db.log.EntryEncodingUtil.skip;
import static c5db.log.EntryEncodingUtil.sumRemaining;

/**
 * A SequentialEntry that can convert itself to and from Protostuff LogEntry objects.
 * In addition, it can serialize itself directly using the supplied Codec, allowing it
 * to be written to an {@link EncodedSequentialLog}. Its serialized form consists of a
 * header together with some content ({@link OLogContent}).
 * <p>
 * An OLogEntry is the kind of entry written to, and retrieved from, {@link OLog}.
 */
public final class OLogEntry extends SequentialEntry {
  private final long electionTerm;
  private final OLogContent content;

  public OLogEntry(long seqNum, long electionTerm, OLogContent content) {
    super(seqNum);

    assert content != null;

    this.electionTerm = electionTerm;
    this.content = content;
  }

  public long getElectionTerm() {
    return electionTerm;
  }

  public OLogContent getContent() {
    return content;
  }

  public OLogContentType getContentType() {
    return content.getType();
  }

  public LogEntry toProtostuff() {
    switch (content.getType()) {
      case DATA:
        return new LogEntry(electionTerm, seqNum, ((OLogRawDataContent) content).getRawData(), null);
      case QUORUM_CONFIGURATION:
        return new LogEntry(electionTerm, seqNum, new ArrayList<>(),
            (QuorumConfigurationMessage) ((OLogProtostuffContent) content).getMessage());
    }

    throw new RuntimeException("OLogEntry#toProtostuff");
  }

  public static OLogEntry fromProtostuff(LogEntry entry) {
    final OLogContent content;
    if (entry.getQuorumConfiguration() != null) {
      content = new OLogProtostuffContent<>(entry.getQuorumConfiguration());
    } else {
      content = new OLogRawDataContent(entry.getDataList());
    }

    return new OLogEntry(entry.getIndex(), entry.getTerm(), content);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || (o.getClass() != this.getClass())) {
      return false;
    }
    OLogEntry that = (OLogEntry) o;
    return this.electionTerm == that.getElectionTerm()
        && this.seqNum == that.getSeqNum()
        && this.content.equals(that.getContent());
  }

  @Override
  public int hashCode() {
    return ((int) seqNum) * 31 +
        ((int) electionTerm) * 31 +
        content.hashCode();
  }

  @Override
  public String toString() {
    return "OLogEntry{" +
        "seqNum=" + seqNum +
        ", electionTerm=" + electionTerm +
        ", content=" + content;
  }

  public static class Codec implements SequentialEntryCodec<OLogEntry> {
    private static final Schema<OLogEntryHeader> SCHEMA = OLogEntryHeader.getSchema();
    // TODO capability of having multiple 4-byte CRCs for large content
    private static final int CRC_BYTES = 4;

    @Override
    public ByteBuffer[] encode(OLogEntry entry) {
      try {
        final List<ByteBuffer> contentBufs = entry.getContent().serialize();
        final int contentLength = sumRemaining(contentBufs);
        final OLogEntryHeader header = createHeader(entry, contentLength);

        final List<ByteBuffer> entryBufs = encodeWithLengthAndCrc(SCHEMA, header);
        entryBufs.addAll(appendCrcToBufferList(contentBufs));

        return Iterables.toArray(entryBufs, ByteBuffer.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public OLogEntry decode(InputStream inputStream) throws IOException, CrcError {
      final OLogEntryHeader header = decodeAndCheckCrc(inputStream, SCHEMA);
      final ByteBuffer contentBuf = getAndCheckContent(inputStream, header.getContentLength());

      return new OLogEntry(
          header.getSeqNum(),
          header.getTerm(),
          OLogContent.deserialize(contentBuf, header.getType()));
    }

    @Override
    public long skipEntryAndReturnSeqNum(InputStream inputStream) throws IOException {
      final OLogEntryHeader header = decodeAndCheckCrc(inputStream, SCHEMA);
      skipContent(inputStream, header.getContentLength());
      return header.getSeqNum();
    }

    public OLogEntryHeader skipEntryAndReturnHeader(InputStream inputStream) throws IOException {
      final OLogEntryHeader header = decodeAndCheckCrc(inputStream, SCHEMA);
      skipContent(inputStream, header.getContentLength());
      return header;
    }

    private void skipContent(InputStream inputStream, int contentLength) throws IOException {
      skip(inputStream, IntMath.checkedAdd(contentLength, CRC_BYTES));
    }

    private static OLogEntryHeader createHeader(OLogEntry entry, int contentLength) {
      return new OLogEntryHeader(
          entry.getSeqNum(),
          entry.getElectionTerm(),
          contentLength,
          entry.getContent().getType());
    }
  }
}
