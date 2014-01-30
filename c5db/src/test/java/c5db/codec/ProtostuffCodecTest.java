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

import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ProtostuffCodecTest {



    private static class SerObj implements Message<SerObj>, Schema<SerObj> {
        private int id;
        private String desc;
        private long timestamp;
        private double cost;

        public SerObj(int id, String desc, long timestamp, double cost) {
            this.id = id;
            this.desc = desc;
            this.timestamp = timestamp;
            this.cost = cost;
        }

        private SerObj() {

        }

        @Override
        public Schema<SerObj> cachedSchema() {
            return this;
        }

        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1: return "id";
                case 2: return "desc";
                case 3: return "timestamp";
                case 4: return "cost";
                default: return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            switch(name) {
                case "id": return 1;
                case "desc": return 2;
                case "timestamp": return 3;
                case "cost": return 4;
                default: return 0;
            }
        }

        @Override
        public boolean isInitialized(SerObj message) {
            return true;
        }

        @Override
        public SerObj newMessage() {
            return new SerObj();
        }

        @Override
        public String messageName() {
            return getClass().getSimpleName();
        }

        @Override
        public String messageFullName() {
            return getClass().getName();
        }

        @Override
        public Class<? super SerObj> typeClass() {
            return SerObj.class;
        }

        @Override
        public void mergeFrom(Input input, SerObj message) throws IOException {
            for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
            {
                switch(number)
                {
                    case 0: return;

                    case 1:
                        message.id = input.readInt32();
                        break;

                    case 2:
                        message.desc = input.readString();
                        break;

                    case 3:
                        message.timestamp = input.readInt64();
                        break;

                    case 4:
                        message.cost = input.readDouble();
                        break;
                }
            }
        }

        @Override
        public void writeTo(Output output, SerObj message) throws IOException {
            output.writeInt32(1, message.id, false);
            output.writeString(2, message.desc, false);
            output.writeInt64(3, message.timestamp, false);
            output.writeDouble(4, message.cost, false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SerObj serObj = (SerObj) o;

            if (Double.compare(serObj.cost, cost) != 0) return false;
            if (id != serObj.id) return false;
            if (timestamp != serObj.timestamp) return false;
            if (!desc.equals(serObj.desc)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = id;
            result = 31 * result + desc.hashCode();
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            temp = Double.doubleToLongBits(cost);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }

    @Test
    public void testSerDe() throws Exception {

        SerObj o = new SerObj(11, "hello world", System.currentTimeMillis(), 4.455);
        ProtostuffEncoder<SerObj> enc = new ProtostuffEncoder<>();
        List<Object> objs = new ArrayList<>();
        enc.encode(null, o, objs);
        assertEquals(1, objs.size());

        ProtostuffDecoder<SerObj> dec = new ProtostuffDecoder<>(o);
        List<Object> results = new ArrayList<>();
        dec.decode(null, (ByteBuf)objs.get(0), results);

        assertEquals(1, results.size());

        SerObj aResult = (SerObj) results.get(0);
        assertEquals(o, aResult);

    }

}
