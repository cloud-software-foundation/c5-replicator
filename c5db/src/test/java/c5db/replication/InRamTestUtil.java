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

package c5db.replication;

import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcWireRequest;
import com.dyuproject.protostuff.Message;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.fibers.Fiber;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class contains static utility methods for working with in-memory replication simulations.
 */
public class InRamTestUtil {

  // Provide a pseudo-synchronous wrapper of asynchronous messaging, for testing.
  static public RpcReply syncSendMessage(Fiber fiber,
                                         long from,
                                         ReplicatorInstance repl,
                                         Message msg) throws InterruptedException {
    final BlockingQueue<RpcReply> queue = new ArrayBlockingQueue<>(1);
    RpcWireRequest request = new RpcWireRequest(from, repl.getQuorumId(), msg);
    AsyncRequest.withOneReply(fiber, repl.getIncomingChannel(), request, (message) -> {
      try {
        queue.put(message);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    return queue.poll(10, TimeUnit.SECONDS);
  }
}