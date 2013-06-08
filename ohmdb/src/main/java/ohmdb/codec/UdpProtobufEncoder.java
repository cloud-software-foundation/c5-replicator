/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.codec;

import com.google.protobuf.MessageLite;
import com.google.protobuf.MessageLiteOrBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.MessageBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.net.InetSocketAddress;

import static io.netty.buffer.Unpooled.wrappedBuffer;

public class UdpProtobufEncoder extends MessageToMessageEncoder<UdpProtobufEncoder.UdpProtobufMessage> {
    public static class UdpProtobufMessage {
        public final InetSocketAddress remoteAddress;
        public final MessageLiteOrBuilder message;

        public UdpProtobufMessage(InetSocketAddress remoteAddress, MessageLiteOrBuilder message) {
            this.remoteAddress = remoteAddress;
            this.message = message;
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, UdpProtobufEncoder.UdpProtobufMessage msg, MessageBuf<Object> out) throws Exception {
        ByteBuf data = null;
        if (msg.message instanceof MessageLite) {
            data = wrappedBuffer(((MessageLite) msg.message).toByteArray());
        }
        if (msg.message instanceof MessageLite.Builder) {
            data = wrappedBuffer(((MessageLite.Builder) msg.message).build().toByteArray());
        }

        out.add(new DatagramPacket(data, msg.remoteAddress));
    }
}
