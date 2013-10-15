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
package ohmdb.replication.rpc;

import com.google.protobuf.MessageLite;

/**
 * An rpc reply in response to a 'wire request'.
 */
public class RpcReply extends RpcMessage {
     /**
      *  Invert the to/from and quote the messageId.
      *
      * @param inReplyTo
      * @param message
      */
    public RpcReply(RpcWireRequest inReplyTo, MessageLite message) {
        super(inReplyTo.from, inReplyTo.to, inReplyTo.messageId, message);
    }
}
