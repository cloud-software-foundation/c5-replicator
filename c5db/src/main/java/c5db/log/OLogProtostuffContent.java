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
import c5db.replication.QuorumConfiguration;
import c5db.replication.generated.QuorumConfigurationMessage;
import com.google.common.collect.ImmutableMap;
import io.protostuff.ByteBufferInput;
import io.protostuff.LinkBuffer;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static c5db.generated.OLogContentType.QUORUM_CONFIGURATION;

/**
 * Content in the form of a protostuff message, which is included in an OLog entry.
 */
public final class OLogProtostuffContent<T extends Schema<T> & Message<T>> extends OLogContent {
  private final T message;

  // Map protostuff typeClass to OLogContentType
  private static final ImmutableMap<Class<?>, OLogContentType> TYPE_MAP =
      new ImmutableMap.Builder<Class<?>, OLogContentType>()
          .put(QuorumConfigurationMessage.getSchema().typeClass(), QUORUM_CONFIGURATION).build();


  public OLogProtostuffContent(T message) {
    super(TYPE_MAP.get(message.typeClass()));
    assert message != null;
    this.message = message;
  }

  public T getMessage() {
    return message;
  }

  @Override
  public List<ByteBuffer> serialize() {
    final LinkBuffer messageBuf = new LinkBuffer();
    final LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput(messageBuf);

    try {
      message.writeTo(lcpo, message);
      return messageBuf.finish();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Schema<T> & Message<T>> OLogProtostuffContent<T> deserialize(ByteBuffer buffer,
                                                                                        Schema<T> schema) {
    final ByteBufferInput input = new ByteBufferInput(buffer, true);
    final T message = schema.newMessage();

    try {
      schema.mergeFrom(input, message);
      return new OLogProtostuffContent<>(message);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "OLogProtostuffContent{" +
        "type=" + this.type +
        " message=" + message +
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

    OLogProtostuffContent that = (OLogProtostuffContent) o;
    if (type == QUORUM_CONFIGURATION) {
      return quorumConfigurationMessageEquals(o);
    }
    return message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return message.hashCode();
  }

  // TODO workaround until protostuff gets a working equals() method -- uses Class.cast()
  private boolean quorumConfigurationMessageEquals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QuorumConfigurationMessage thisMessage = QuorumConfigurationMessage.class.cast(message);
    QuorumConfigurationMessage thatMessage = QuorumConfigurationMessage.class.cast(((OLogProtostuffContent) o).message);

    return QuorumConfiguration.fromProtostuff(thisMessage).equals(
        QuorumConfiguration.fromProtostuff(thatMessage)
    );
  }
}
