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
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Raw data included in an OLog entry. OLog does not know or care about the type or internal
 * structure of the data. Serialization and deserialization is trivial.
 */
public final class OLogRawDataContent extends OLogContent {
  private final List<ByteBuffer> rawData;


  public OLogRawDataContent(List<ByteBuffer> rawData) {
    super(OLogContentType.DATA);
    assert rawData != null;
    this.rawData = sliceAll(rawData);
  }

  public List<ByteBuffer> getRawData() {
    return rawData;
  }

  public List<ByteBuffer> serialize() {
    return rawData;
  }

  public static OLogContent deserialize(ByteBuffer buffer) {
    return new OLogRawDataContent(Lists.newArrayList(buffer));
  }

  @Override
  public String toString() {
    return "OLogRawDataContent{" +
        "rawData=" + rawData +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OLogRawDataContent that = (OLogRawDataContent) o;
    return rawData.equals(that.rawData);
  }

  @Override
  public int hashCode() {
    return rawData.hashCode();
  }


  private static List<ByteBuffer> sliceAll(List<ByteBuffer> buffers) {
    return Lists.transform(buffers, ByteBuffer::slice);
  }
}
