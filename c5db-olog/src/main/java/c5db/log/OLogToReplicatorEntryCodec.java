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
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.interfaces.replication.ReplicatorEntry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A Codec to decode entries that were written as OLogEntry, and return them as ReplicatorEntry,
 * ignoring any OLogEntry except for ones whose type is DATA.
 */
public class OLogToReplicatorEntryCodec implements SequentialEntryCodec<ReplicatorEntry> {
  private OLogEntry.Codec oLogEntryCodec = new OLogEntry.Codec();

  @Override
  public ByteBuffer[] encode(ReplicatorEntry entry) {
    throw new UnsupportedOperationException("OLogToReplicatorEntryCodec is read-only");
  }

  @Override
  public ReplicatorEntry decode(InputStream inputStream) throws IOException, EntryEncodingUtil.CrcError {
    OLogEntry oLogEntry;

    do {
      oLogEntry = oLogEntryCodec.decode(inputStream);
    } while (oLogEntry.getContentType() != OLogContentType.DATA);

    return new ReplicatorEntry(oLogEntry.getSeqNum(), oLogEntry.toProtostuff().getDataList());
  }

  @Override
  public long skipEntryAndReturnSeqNum(InputStream inputStream) throws IOException, EntryEncodingUtil.CrcError {
    OLogEntryHeader oLogEntryHeader;

    do {
      oLogEntryHeader = oLogEntryCodec.skipEntryAndReturnHeader(inputStream);
    } while (oLogEntryHeader.getType() != OLogContentType.DATA);

    return oLogEntryHeader.getSeqNum();
  }
}
