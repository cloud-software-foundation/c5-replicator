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

import com.dyuproject.protostuff.LowCopyProtobufOutput;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Schema;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Serialized a protostuff object - using 'protobuf' format
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
