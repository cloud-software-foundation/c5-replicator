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
import io.netty.handler.codec.MessageToMessageDecoder;
import io.protostuff.ByteBufferInput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.util.List;

/**
 * Decode a protobuf object using the "protostuff" library.
 * The replication library uses this class to decode replication messages over the wire.
 */
public class ProtostuffDecoder<T extends Message<T>> extends MessageToMessageDecoder<ByteBuf> {
  final Schema<T> schema;

  public ProtostuffDecoder(Schema<T> schema) {
    this.schema = schema;
  }


  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    ByteBufferInput input = new ByteBufferInput(in.nioBuffer(), false);
    T newMsg = schema.newMessage();

    schema.mergeFrom(input, newMsg);
    out.add(newMsg);
  }
}
