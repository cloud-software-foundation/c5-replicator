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

import c5db.client.generated.Action;
import c5db.client.generated.Call;
import c5db.client.generated.Get;
import c5db.client.generated.GetResponse;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MultiResponse;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutateResponse;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.Response;
import c5db.client.generated.ScanRequest;
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

/**
 * The main netty handler for the RegionServer functionality. Maps protocol buffer calls to an action against a HRegion
 * and then provides a response to the caller.
 */
public class C5ServerHandler extends
    SimpleChannelInboundHandler<Call> {
  private final RegionServerService regionServerService;
  ScannerManager scanManager = ScannerManager.INSTANCE;

  public C5ServerHandler(RegionServerService myService) {
    this.regionServerService = myService;
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx,
                           final Call call)
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

  private void multi(ChannelHandlerContext ctx, Call call)
      throws IOException {
    MultiRequest request = call.getMulti();
    MultiResponse multiResponse = new MultiResponse();

    List<MutationProto> mutations = new ArrayList<>();

    for (RegionAction regionAction : request.getRegionActionList()) {

      for (Action actionUnion : regionAction.getActionList()) {
        if (actionUnion.getMutation() != null) {
          mutations.add(actionUnion.getMutation());
        } else {
          throw new IOException("Unsupported atomic action type: " + actionUnion);
        }
      }
    }
    if (!mutations.isEmpty()) {
      MutationProto firstMutate = mutations.get(0);
      byte[] row = firstMutate.getRow().array();
      RowMutations rm = new RowMutations(row);
      for (MutationProto mutate : mutations) {
        MutationProto.MutationType type = mutate.getMutateType();
        switch (mutate.getMutateType()) {
          case PUT:
            rm.add(ReverseProtobufUtil.toPut(mutate));
            break;
          case DELETE:
            rm.add(ReverseProtobufUtil.toDelete(mutate));
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
    Response response = new Response(Response.Command.MULTI,
        call.getCommandId(),
        null,
        null,
        null,
        multiResponse);
    ctx.writeAndFlush(response);
  }

  private void mutate(ChannelHandlerContext ctx, Call call)
      throws IOException {
    MutateRequest mutateIn = call.getMutate();
    MutateResponse mutateResponse;
    try {
      HRegion region = regionServerService.getOnlineRegion("1");
      switch (mutateIn.getMutation().getMutateType()) {
        case PUT:
          region.put(ReverseProtobufUtil.toPut(mutateIn.getMutation()));
          break;
        case DELETE:
          region.delete(ReverseProtobufUtil.toDelete(mutateIn.getMutation()));
          break;
      }
      mutateResponse = new MutateResponse(null, true);
    } catch (IOException e) {
      mutateResponse = new MutateResponse(null, false);
      e.printStackTrace();
    }

    Response response = new Response(Response.Command.MUTATE,
        call.getCommandId(),
        null,
        mutateResponse,
        null,
        null);
    ctx.writeAndFlush(response);
  }

  private void scan(ChannelHandlerContext ctx, Call call)
      throws IOException {

    ScanRequest scanIn = call.getScan();

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

  private long getScannerId(ScanRequest scanIn, long scannerId) {
    if (scanIn.getScannerId() > 0) {
      scannerId = scanIn.getScannerId();
    } else {
      // Make a scanner with an Id not 0
      do {
        scannerId = System.currentTimeMillis();
      } while (scannerId == 0);
    }
    return scannerId;
  }

  private void get(ChannelHandlerContext ctx, Call call)
      throws IOException {
    Get getIn = call.getGet().getGet();

    HRegion region = regionServerService.getOnlineRegion("1");
    Result regionResult = region.get(ReverseProtobufUtil.toGet(getIn));
    c5db.client.generated.Result result;

    if (getIn.getExistenceOnly()) {
      result = new c5db.client.generated.Result(new ArrayList<>(), 0, regionResult.getExists());
    } else {
      result = ReverseProtobufUtil.toResult(regionResult);
    }

    GetResponse getResponse = new GetResponse(result);

    Response response = new Response(Response.Command.GET, call.getCommandId(), getResponse, null, null, null);
    ctx.writeAndFlush(response);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

}