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

import c5db.generated.OLogData;
import c5db.generated.OLogMetaData;
import c5db.generated.QuorumMapping;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
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

        List<ByteBuffer> kvs = new ArrayList<ByteBuffer>();
        kvs.add(ByteBuffer.wrap("foo".getBytes()));
        kvs.add(ByteBuffer.wrap("bar".getBytes()));

        OLogData datum = new OLogData();
        datum.setLogData(new OLogMetaData().setIndex(123).setQuorumTag(456).setTerm(789));
        datum.setQuorumTagMapping(new QuorumMapping().setQuorumTag(1).setQuorumId("foo"));
        datum.setKvsList(kvs);


        Schema<OLogData> schema = RuntimeSchema.getSchema(OLogData.class);
        LinkedBuffer buffer = LinkedBuffer.allocate(256);
        {
            long sum=0;
            int size=0;

            for (int i = 0 ; i < 100;i++) {
                long start = System.nanoTime();

                size = ProtostuffIOUtil.writeTo(buffer, datum, schema);
//                ProtobufIOUtil.writeTo(buffer, datum, schema);

                long tim = System.nanoTime() - start;

                buffer.clear();

                if (i>10) {
                    sum += tim;
                }
            }


            System.out.println("sum = " + sum + " avg: " + (sum / 99) + " siz: " + size);
        }
    }


}
