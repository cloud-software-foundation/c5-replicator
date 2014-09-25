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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Serialized a protostuff object - using 'protobuf' format.
 * The replication library uses this class to decode replication messages over the wire.
 */
public class ProtostuffEncoder<T extends Message<T>> extends MessageToMessageEncoder<Message<T>> {
  @Override
  protected void encode(ChannelHandlerContext ctx, Message<T> msg, List<Object> out) throws Exception {
    Schema<T> schema = msg.cachedSchema();

    LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput();
    schema.writeTo(lcpo, (T) msg);

    List<ByteBuffer> buffers = lcpo.buffer.finish();

    long size = lcpo.buffer.size();
    if (size > Integer.MAX_VALUE) {
      throw new EncoderException("Serialized form was too large, actual size: " + size);
    }

    out.add(Unpooled.wrappedBuffer(buffers.toArray(new ByteBuffer[]{})));
  }
}
