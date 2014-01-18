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
package c5db.regionserver;

import c5db.client.generated.ClientProtos;
import c5db.regionserver.scanner.ScanRunnable;
import c5db.regionserver.scanner.ScannerManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OhmServerHandler extends
        SimpleChannelInboundHandler<ClientProtos.Call> {
    ScannerManager scanManager = ScannerManager.INSTANCE;

    private final RegionServerService regionServerService;
    public OhmServerHandler(RegionServerService myService) {
        this.regionServerService = myService;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx,
                             final ClientProtos.Call call)
            throws Exception {
        switch (call.getCommand()) {
            case GET:
                get(ctx, call);
                break;
            case MUTATE:
                mutate(ctx, call);
                break;
            case SCAN:
                scan(ctx, call);
                break;
            case MULTI:
                multi(ctx, call);
                break;
        }
    }

    private void multi(ChannelHandlerContext ctx, ClientProtos.Call call)
            throws IOException {
        ClientProtos.MultiRequest request = call.getMulti();
        ClientProtos.MultiResponse.Builder multiResponse =
                ClientProtos.MultiResponse.newBuilder();


        List<ClientProtos.MutationProto> mutations = new ArrayList<>();
        for (int i = 0; i != request.getRegionActionCount(); i++) {
            ClientProtos.RegionAction regionAction = request.getRegionAction(i);
            for (ClientProtos.Action actionUnion : regionAction.getActionList()) {
                if (actionUnion.hasMutation()) {
                    mutations.add(actionUnion.getMutation());
                } else {
                    throw new IOException("Unsupported atomic action type: " + actionUnion);
                }
            }
        }
        if (!mutations.isEmpty()) {
            ClientProtos.MutationProto firstMutate = mutations.get(0);
            byte[] row = firstMutate.getRow().toByteArray();
            RowMutations rm = new RowMutations(row);
            for (ClientProtos.MutationProto mutate : mutations) {
                ClientProtos.MutationProto.MutationType type = mutate.getMutateType();
                switch (mutate.getMutateType()) {
                    case PUT:
                        rm.add(ReverseProtobufUtil.toPut(mutate, null));
                        break;
                    case DELETE:
                        rm.add(ReverseProtobufUtil.toDelete(mutate, null));
                        break;
                    default:
                        throw new RuntimeException(
                                "mutate supports atomic put and/or delete, not "
                                        + type.name());
                }
            }
            HRegion region = regionServerService.getOnlineRegion("1");
            region.mutateRow(rm);
        }
        ClientProtos.Response response = ClientProtos
                .Response
                .newBuilder()
                .setCommand(ClientProtos.Response.Command.MULTI)
                .setCommandId(call.getCommandId())
                .setMulti(multiResponse.build()).build();
        ctx.writeAndFlush(response);
    }

    private void mutate(ChannelHandlerContext ctx, ClientProtos.Call call)
            throws IOException {
        ClientProtos.MutateRequest mutateIn = call.getMutate();
        ClientProtos.MutateResponse.Builder mutateResponse =
                ClientProtos.MutateResponse.newBuilder();
        try {
            HRegion region = regionServerService.getOnlineRegion("1");
            switch (mutateIn.getMutation().getMutateType()) {
                case PUT:
                    region.put(ReverseProtobufUtil.toPut(mutateIn.getMutation(), null));
                    break;
                case DELETE:
                    region.delete(ReverseProtobufUtil.toDelete(mutateIn.getMutation(), null));
                    break;
            }
            mutateResponse.setProcessed(true);
        } catch (IOException e) {
            mutateResponse.setProcessed(false);
            e.printStackTrace();
        }

        ClientProtos.Response response = ClientProtos
                .Response
                .newBuilder()
                .setCommand(ClientProtos.Response.Command.MUTATE)
                .setCommandId(call.getCommandId())
                .setMutate(mutateResponse.build()).build();
        ctx.writeAndFlush(response);
    }

    private void scan(ChannelHandlerContext ctx, ClientProtos.Call call)
            throws IOException {

        ClientProtos.ScanRequest scanIn = call.getScan();

        long scannerId = 0;
        scannerId = getScannerId(scanIn, scannerId);
        Integer numberOfRowsToSend = scanIn.getNumberOfRows();

        Channel<Integer> channel = scanManager.getChannel(scannerId);
        // New Scanner
        if (null == channel) {
            Fiber fiber = new ThreadFiber();
            fiber.start();
            channel = new MemoryChannel<>();

            ScanRunnable scanRunnable = new ScanRunnable(ctx, call, scannerId,
                    regionServerService.getOnlineRegion("1"));
            channel.subscribe(fiber, scanRunnable);
            scanManager.addChannel(scannerId, channel);
        }
        channel.publish(numberOfRowsToSend);
    }

    private long getScannerId(ClientProtos.ScanRequest scanIn, long scannerId) {
        if (scanIn.hasScannerId() && scanIn.getScannerId() > 0) {
            scannerId = scanIn.getScannerId();
        } else {
            // Make a scanner with an Id not 0
            while (scannerId == 0) {
                scannerId = System.currentTimeMillis();
            }
        }
        return scannerId;
    }

    private void get(ChannelHandlerContext ctx, ClientProtos.Call call)
            throws IOException {
        ClientProtos.Get getIn = call.getGet().getGet();
        ClientProtos.GetResponse.Builder getResponse =
                ClientProtos.GetResponse.newBuilder();
        HRegion region = regionServerService.getOnlineRegion("1");
        Result result = region.get(ReverseProtobufUtil.toGet(getIn));
        if (! getIn.getExistenceOnly()){
            getResponse.setResult(ReverseProtobufUtil.toResult(result));
        } else {
            getResponse.setResult(ClientProtos.Result.newBuilder().setExists(result.getExists()));
        }
        ClientProtos.Response response = ClientProtos
                .Response
                .newBuilder()
                .setCommand(ClientProtos.Response.Command.GET)
                .setCommandId(call.getCommandId())
                .setGet(getResponse.build()).build();

        ctx.writeAndFlush(response);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

}


