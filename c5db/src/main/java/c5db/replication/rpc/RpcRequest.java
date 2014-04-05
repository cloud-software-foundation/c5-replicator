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
package c5db.replication.rpc;


import io.protostuff.Message;

/**
 * An outbound request for the transport.  Since the transport knows who 'we' are, the only
 * params required is a 'to' and which quorumId is being involved.  Oh yes and the actual message.
 *
 * Actually scratch that, apparently certain types of transports (InRamSim) don't know who 'we' are.  So include that.
 */
public class RpcRequest extends RpcMessage {

    public RpcRequest(long to, long from, String quorumId, Message message) {
        // Note that the RPC system should sub in a message id, that is an implementation detail
        // since not all transports (eg: in RAM only transport) need message IDs to keep request/replies in line.
        super(to, from, quorumId, message);
    }
}

