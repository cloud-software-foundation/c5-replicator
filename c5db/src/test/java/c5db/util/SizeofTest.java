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
package c5db.util;

import c5db.discovery.generated.Availability;
import c5db.discovery.generated.Beacon;
import c5db.discovery.generated.ModuleDescriptor;
import c5db.messages.generated.ControlMessages;
import c5db.messages.generated.ModuleType;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import com.google.protobuf.CodedOutputStream;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Sizeof test using SizeOf.jar
 */
public class SizeofTest {


    @BeforeClass
    public static void setup() {
//        SizeOf.skipStaticField(true);
//        SizeOf.skipFlyweightObject(true);
        //SizeOf.turnOnDebug();
    }

    //@Test
    public void testSize() throws Exception {

        ByteBuffer bb = ByteBuffer.wrap(new byte[10]);

        printsizes("bb(new byte[10])", bb);

        ByteBuffer bba = ByteBuffer.allocate(100);
        printsizes("ByteBuffer(100)", bba);

        ByteBuffer bb2  = ByteBuffer.allocate(0);
        printsizes("bb(0)", bb2);
        ByteBuffer bb3 = ByteBuffer.allocateDirect(10);
        printsizes("bb.direct(10)", bb3);

        byte[] foo = new byte[0];
        printsizes("byte[0]", foo);

        byte[] foo2 = new byte[1];
        printsizes("byte[1]", foo2);

        KV kv = new KV(foo);
        printsizes("KV(byte[0])", kv);

        KV kv2 = new KV(foo2);
        printsizes("KV(byte[1])", kv2);

        //System.out.println("Sizeof base array: " + SizeOf.sizeOf(new byte[0]));
    }

    private static class KV {
        byte[] datas;
        public KV(byte[] datas) {
            this.datas = datas;
        }
    }

    public void testAThing(Function f) {

    }


    @Test
    public void testSizeProto() throws Exception {
        Beacon.Availability.Builder beaconMessage = Beacon.Availability.newBuilder()
                .addAddresses("127.0.0.1")
                .setNodeId(1234);
        List<Beacon.ModuleDescriptor> msgModules = new ArrayList<>(ControlMessages.ModuleType.values().length);
        for (ControlMessages.ModuleType type : ControlMessages.ModuleType.values()) {
            msgModules.add(Beacon.ModuleDescriptor.newBuilder()
            .setModule(type)
            .setModulePort(1111).build());
        }
        beaconMessage.addAllModules(msgModules);

        // now sizeof this fucker:
        printsizes("protobuf(builder)", beaconMessage);
        Beacon.Availability msg = beaconMessage.build();
        printsizes("protobuf(obj)", msg);

        ByteBufferOutputStream outputStream = new ByteBufferOutputStream(1024);

        long sum = 0;
        for (int i = 0; i < 200; i++) {

            CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream,
                    1024);

            long start = System.nanoTime();

            msg.writeTo(codedOutputStream);
            codedOutputStream.flush();

//            byte[] ser = msg.toByteArray();
            long tim = System.nanoTime() - start;
//            System.out.println("protobuf size= " + ser.length + " Time = " + tim);
            if (i > 0) {
                sum += tim;
            }
            outputStream.reset();

        }

        System.out.println("sum = " + sum + " avg: " + (sum / 199));
        double protobuftime = (double)sum/199.0;

        // now PROTOSTUFF:
        List<ModuleDescriptor> msgModules2 = new ArrayList<>(ModuleType.values().length);
        for (ModuleType type : ModuleType.values()) {
            msgModules2.add(new ModuleDescriptor(type, 1111));
        }

        Availability beaconMsg2 = new Availability(1234L, 0,
                Collections.singletonList("127.0.0.1"),
                        msgModules2);

        LinkedBuffer buf = LinkedBuffer.allocate(256);
        sum = 0;

        for (int i = 0; i < 200; i++) {

            long start = System.nanoTime();
            int len = ProtostuffIOUtil.writeTo(buf, beaconMsg2, beaconMsg2);
            //byte[] ser2 = ProtobufIOUtil.toByteArray( buf);
            long tim = System.nanoTime() - start;

//            System.out.println("protostuff size= " + len + " Time = " + tim);
            buf.clear();

            if (i > 0) {
                sum += tim;
            }

        }
        System.out.println("sum = " + sum + " avg: " + (sum / 199));
        double protostufftime = (double)sum / 199.0;

        double diff = Math.max(protostufftime, protobuftime) - Math.min(protostufftime, protobuftime);
        double p1 = (diff / Math.max(protostufftime, protobuftime))*100.0;
        double p2 = (diff / Math.min(protostufftime, protobuftime))*100.0;
        System.out.println("Diff in time: " + diff + " percent1: " + p1 + "%, percent2: " + p2 + "%");

        printsizes("protoSTUFF", beaconMsg2);
    }

    private void printsizes(String thing, Object obj) {
        System.out.println("Size of '"  + thing + "' not available (fix src)");
    }
//    private void printsizes(String thing, Object obj) {
//        System.out.println("Sizeof '" + thing + "': " +
//        SizeOf.sizeOf(obj) + " deep sizeof: " + SizeOf.deepSizeOf(obj));
//    }
}
