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
