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
package c5db.log;

import c5db.generated.Log;
import c5db.generated.OLogData;
import c5db.generated.OLogMetaData;
import c5db.generated.QuorumMapping;
import io.protostuff.ByteBufferInput;
import io.protostuff.LinkedBuffer;
import io.protostuff.LowCopyProtostuffOutput;
import io.protostuff.ProtostuffIOUtil;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Random experiments in protostuff serialization.
 */
public class ProtostuffTest {
    @Test
    public void testThing() throws Exception {

        List<ByteBuffer> kvs = new ArrayList<>();
        kvs.add(ByteBuffer.wrap("foo".getBytes()));
        kvs.add(ByteBuffer.wrap("bar".getBytes()));

        OLogMetaData oLogMetaData = new OLogMetaData(11, 456, 99999);
        QuorumMapping qm = new QuorumMapping(1, "foos");
        OLogData datum = new OLogData(oLogMetaData, qm, kvs);

        Log.OLogMetaData metaData = Log.OLogMetaData.newBuilder()
                .setIndex(456)
                .setQuorumTag(11)
                .setTerm(99999)
                .build();
        Log.QuorumMapping qm2 = Log.QuorumMapping.newBuilder()
                .setQuorumTag(1)
                .setQuorumId("foo")
                .build();
        List<ByteString> kvs2 = new ArrayList<>();
        kvs2.add(ByteString.copyFrom("foo".getBytes()));
        kvs2.add(ByteString.copyFrom("bar".getBytes()));
        Log.OLogData datum2 = Log.OLogData.newBuilder()
                .setLogData(metaData)
                .setQuorumTagMapping(qm2)
                .addAllKvs(kvs2)
                .build();

        byte[] protobufOutput = datum2.toByteArray();

        ByteBufferInput protoInput = new ByteBufferInput(
                ByteBuffer.wrap(protobufOutput), false);
        OLogData readDatum = new OLogData();
        readDatum.mergeFrom(protoInput, readDatum);
        System.out.println(readDatum.toString());

        LinkedBuffer buffer = LinkedBuffer.allocate(256);
        {
            long sum=0;
            int size=0;

            for (int i = 0 ; i < 200;i++) {
                long start = System.nanoTime();

                size = ProtostuffIOUtil.writeTo(buffer, datum, datum);
//                ProtobufIOUtil.writeTo(buffer, datum, schema);

                long tim = System.nanoTime() - start;

                buffer.clear();

                if (i>10) {
                    sum += tim;
                }
            }


            System.out.println("sum = " + sum + " avg: " + (sum / 190) + " siz: " + size);
        }

        {
            long sum=0;
            int size=0;

            for (int i = 0 ; i < 200;i++) {
                long start = System.nanoTime();

                LowCopyProtostuffOutput lcpo = new LowCopyProtostuffOutput();
                datum.writeTo(lcpo, datum);
                size = (int) lcpo.buffer.size();
//                ProtobufIOUtil.writeTo(buffer, datum, schema);

                long tim = System.nanoTime() - start;

                buffer.clear();

                if (i>10) {
                    sum += tim;
                }
            }


            System.out.println("sum = " + sum + " avg: " + (sum / 190) + " siz: " + size);
        }



    }


}
