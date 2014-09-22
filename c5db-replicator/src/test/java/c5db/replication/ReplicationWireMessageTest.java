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

package c5db.replication;

import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.generated.RequestVote;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.protostuff.LinkBuffer;
import io.protostuff.LowCopyProtobufOutput;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class ReplicationWireMessageTest {

  @Test
  public void testSimpleSerialization() throws Exception {
    RequestVote rv = new RequestVote(1, 22222, 34, 22);
    ReplicationWireMessage rwm = new ReplicationWireMessage(
        1, 1, 0, "quorumId", false, rv, null, null, null, null, null
    );

    LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput(new LinkBuffer(24));
    rwm.writeTo(lcpo, rwm);
    List<ByteBuffer> serBufs = lcpo.buffer.finish();
    logBufsInformation("ReplicationWireMessage", serBufs);

    ByteBuf b = Unpooled.wrappedBuffer(serBufs.toArray(new ByteBuffer[serBufs.size()]));
    System.out.println("ByteBuf info = " + b);
    System.out.println("ByteBuf size = " + b.readableBytes());
    assertEquals(lcpo.buffer.size(), b.readableBytes());

    System.out.println("rwm = " + rwm);
  }

  void logBufsInformation(String desc, List<ByteBuffer> buffs) {
    System.out.println(desc + ": buffer count = " + buffs.size());
    long size = 0;
    for (ByteBuffer b : buffs) {
      System.out.println(desc + ": buff=" + b);
      size += b.remaining();
    }
    System.out.println(desc + ": totalSize = " + size);
  }
}
