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
import c5db.replication.generated.QuorumConfigurationMessage;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Abstraction representing content included in an OLog entry. It has a type (specified in
 * the header of the entry), and the ability to serialize or deserialize itself.
 */
public abstract class OLogContent {
  protected final OLogContentType type;

  protected OLogContent(OLogContentType type) {
    this.type = type;
  }

  public OLogContentType getType() {
    return type;
  }

  public abstract List<ByteBuffer> serialize();

  public static OLogContent deserialize(ByteBuffer buffer, OLogContentType type) {
    switch (type) {
      case DATA:
        return OLogRawDataContent.deserialize(buffer);
      case QUORUM_CONFIGURATION:
        return OLogProtostuffContent.deserialize(buffer, QuorumConfigurationMessage.getSchema());
    }

    throw new RuntimeException("OLogContent#deserialize");
  }

}
