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
