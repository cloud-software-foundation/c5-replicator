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

import c5db.replication.generated.LogEntry;
import com.google.common.collect.Lists;
import io.netty.util.CharsetUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Helper methods to create and manipulate OLogEntry instances.
 */
public class LogTestUtil {

  public static List<OLogEntry> emptyEntryList() {
    return Lists.newArrayList();
  }

  public static OLogEntry makeEntry(long index, long term, String stringData) {
    return makeEntry(index, term, ByteBuffer.wrap(stringData.getBytes(CharsetUtil.UTF_8)));
  }

  public static OLogEntry makeEntry(long index, long term, ByteBuffer data) {
    return new OLogEntry(index, term, Lists.newArrayList(data.duplicate()));
  }

  public static List<OLogEntry> makeSingleEntryList(long index, long term, String stringData) {
    return Lists.newArrayList(makeEntry(index, term, stringData));
  }

  public static List<OLogEntry> makeSingleEntryList(long index, long term, ByteBuffer data) {
    return Lists.newArrayList(makeEntry(index, term, data));
  }

  public static LogEntry makeProtostuffEntry(long index, long term, String stringData) {
    return makeEntry(index, term, stringData).toProtostuffMessage();
  }

  public static LogEntry makeProtostuffEntry(long index, long term, ByteBuffer data) {
    return makeEntry(index, term, data).toProtostuffMessage();
  }
}
