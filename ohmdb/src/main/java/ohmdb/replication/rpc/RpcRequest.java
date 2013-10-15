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
 * RPC Requests from the client => dont need a message id.
 * RPC Requests from the WIRE => message id already comes with.
 *
 * An outbound request for the sender subsystem.
 */
public class RpcRequest extends RpcMessage {

    public RpcRequest(long to, long from, MessageLite message) {
        // Note that the RPC system should sub in a message id, that is an implementation detail
        // since not all transports (eg: in RAM only transport) need message IDs to keep request/replies in line.
        super(to, from, 0, message);
    }
}

