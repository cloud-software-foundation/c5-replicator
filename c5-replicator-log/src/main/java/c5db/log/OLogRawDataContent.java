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

import c5db.log.generated.OLogContentType;
import com.google.common.base.Function;
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
    return Lists.transform(buffers, new Function<ByteBuffer, ByteBuffer>() {
      @Override
      public ByteBuffer apply(ByteBuffer byteBuffer) {
        return byteBuffer.slice();
      }
    });
  }
}
