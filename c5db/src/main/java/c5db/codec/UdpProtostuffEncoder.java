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

import com.dyuproject.protostuff.LinkBuffer;
import com.dyuproject.protostuff.LowCopyProtobufOutput;
import com.dyuproject.protostuff.LowCopyProtostuffOutput;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Schema;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A specialized Protostuff decoder used to serialize Protostuff into a  DatagramPackets and map them to an arbitrary
 * protostuff Message
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
