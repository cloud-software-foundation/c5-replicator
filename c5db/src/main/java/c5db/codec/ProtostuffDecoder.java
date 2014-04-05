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

import io.protostuff.ByteBufferInput;
import io.protostuff.Message;
import io.protostuff.Schema;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

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
