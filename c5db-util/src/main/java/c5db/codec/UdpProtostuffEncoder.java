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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.protostuff.LinkBuffer;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.LowCopyProtostuffOutput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A specialized Protostuff decoder used to serialize Protostuff into a  DatagramPackets and map them to an arbitrary
 * protostuff Message
 *
 * @param <T> The type of message to encode.
 */
public class UdpProtostuffEncoder<T extends Message<T>> extends MessageToMessageEncoder<UdpProtostuffEncoder.UdpProtostuffMessage<T>> {
  private final Schema<T> schema;
  private final boolean protostuffOutput;
  private final int bufferAllocSize;

  public UdpProtostuffEncoder(Schema<T> schema, boolean protostuffOutput) {
    this(schema, protostuffOutput, 512);
  }

  private UdpProtostuffEncoder(Schema<T> schema, boolean protostuffOutput, int bufferAllocSize) {
    this.schema = schema;
    this.protostuffOutput = protostuffOutput;
    this.bufferAllocSize = 512;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, UdpProtostuffMessage<T> msg, List<Object> out) throws Exception {
    LinkBuffer buffer = new LinkBuffer(bufferAllocSize);
    if (protostuffOutput) {
      LowCopyProtostuffOutput lcpo = new LowCopyProtostuffOutput(buffer);
      schema.writeTo(lcpo, msg.message);
    } else {
      LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput(buffer);
      schema.writeTo(lcpo, msg.message);
    }

    List<ByteBuffer> buffers = buffer.finish();
    ByteBuf data = Unpooled.wrappedBuffer(buffers.toArray(new ByteBuffer[buffers.size()]));
    data.retain();

    DatagramPacket dg = new DatagramPacket(data, msg.remoteAddress);
    dg.retain();
    out.add(dg);
  }

  public static class UdpProtostuffMessage<Q extends Message<Q>> {
    public final InetSocketAddress remoteAddress;
    public final Q message;

    public UdpProtostuffMessage(InetSocketAddress remoteAddress, Q message) {
      this.remoteAddress = remoteAddress;
      this.message = message;
    }
  }

}
