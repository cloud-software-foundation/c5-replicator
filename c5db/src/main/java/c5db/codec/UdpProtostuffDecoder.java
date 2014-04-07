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

package c5db.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.protostuff.ByteBufferInput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.util.List;

/**
 * A specialized Protostuff decoder used to de-serialize Protostuff DatagramPackets and map them to an arbitrary
 * protostuff Message
 *
 * @param <T> The type of message to decode.
 */
public class UdpProtostuffDecoder<T extends Message<T>> extends MessageToMessageDecoder<DatagramPacket> {
  final Schema<T> schema;
  final boolean protostuffEncoded;

  /**
   * Netty decoder for protostuff/protobuf messages.
   *
   * @param schema            the schema we are to decode based on
   * @param protostuffEncoded if we are expecting a protostuff object (vs protobuf = false)
   */
  public UdpProtostuffDecoder(Schema<T> schema, boolean protostuffEncoded) {
    this.schema = schema;
    this.protostuffEncoded = protostuffEncoded;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, DatagramPacket dgram, List<Object> out) throws Exception {
    ByteBuf msg = dgram.content();

    ByteBufferInput input = new ByteBufferInput(msg.nioBuffer(), protostuffEncoded);
    T newMsg = schema.newMessage();
    schema.mergeFrom(input, newMsg);
    out.add(newMsg);
  }
}
