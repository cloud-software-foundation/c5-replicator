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

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class UdpProtobufDecoder extends MessageToMessageDecoder<DatagramPacket> {
  private static final boolean HAS_PARSER;

  static {
    boolean hasParser = false;
    try {
      // MessageLite.getParsetForType() is not available until protobuf 2.5.0.
      MessageLite.class.getDeclaredMethod("getParserForType");
      hasParser = true;
    } catch (Throwable t) {
      // Ignore
    }

    HAS_PARSER = hasParser;
  }

  private final MessageLite prototype;
  private final ExtensionRegistry extensionRegistry;

  public UdpProtobufDecoder(MessageLite prototype) {
    this(prototype, null);
  }

  public UdpProtobufDecoder(MessageLite prototype, ExtensionRegistry extensionRegistry) {
    if (prototype == null) {
      throw new NullPointerException("prototype");
    }
    this.prototype = prototype.getDefaultInstanceForType();
    this.extensionRegistry = extensionRegistry;

  }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket dgram, List<Object> out) throws Exception {
        ByteBuf msg = dgram.content();

        final byte[] array;
        final int offset;
        final int length = msg.readableBytes();
        if (msg.hasArray()) {
            array = msg.array();
            offset = msg.arrayOffset() + msg.readerIndex();
        } else {
            array = new byte[length];
            msg.getBytes(msg.readerIndex(), array, 0, length);
            offset = 0;
        }

        if (extensionRegistry == null) {
     //       if (HAS_PARSER) {
     //TODO REPAIR           out.add(prototype.getParserForType().parseFrom(array, offset, length));
     //       } else {
                out.add(prototype.newBuilderForType().mergeFrom(array, offset, length).build());
     //       }
        } else {
      //      if (HAS_PARSER) {
              //TODO REPAIR           out.add(prototype.getParserForType().parseFrom(array, offset, length, extensionRegistry));
     //       } else {
                out.add(prototype.newBuilderForType().mergeFrom(array, offset, length, extensionRegistry).build());
      //      }
        }
    }
}
