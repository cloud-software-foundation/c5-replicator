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
