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
import c5db.client.generated.Condition;
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
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main netty handler for the RegionServer functionality. Maps protocol buffer calls to an action against a HRegion
 * and then provides a response to the caller.
 */
public class C5ServerHandler extends SimpleChannelInboundHandler<Call> {
  private static final Logger LOG = LoggerFactory.getLogger(C5ServerHandler.class);
  private final RegionServerService regionServerService;
  private final ScannerManager scanManager = ScannerManager.INSTANCE;

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
      default:
        LOG.error("Unsupported command:" + call.getCommand());
        LOG.error("Call:" + call);
        break;
    }
  }

  private void multi(ChannelHandlerContext ctx, Call call)
      throws IOException {
    final MultiRequest request = call.getMulti();
    final MultiResponse multiResponse = new MultiResponse();
    final List<MutationProto> mutations = new ArrayList<>();

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
      final MutationProto firstMutate = mutations.get(0);
      final byte[] row = firstMutate.getRow().array();
      final RowMutations rm = new RowMutations(row);
      for (MutationProto mutate : mutations) {
        final MutationProto.MutationType type = mutate.getMutateType();
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
                    + type.name()
            );
        }
      }
      final HRegion region = regionServerService.getOnlineRegion(request.getRegionActionList().get(0).getRegion());
      region.mutateRow(rm);
    }
    final Response response = new Response(Response.Command.MULTI,
        call.getCommandId(),
        null,
        null,
        null,
        multiResponse);
    ctx.writeAndFlush(response);
  }

  private void mutate(ChannelHandlerContext ctx, Call call) {
    boolean success;
    final MutateRequest mutateIn = call.getMutate();
    MutateResponse mutateResponse;
    try {
      final HRegion region = regionServerService.getOnlineRegion(call.getMutate().getRegion());
      if (region == null) {
        throw new IOException("Unable to find region");
      }
      final MutationProto.MutationType type = mutateIn.getMutation().getMutateType();
      switch (type) {
        case PUT:
          if (mutateIn.getCondition().getRow() == null) {
            success = simplePut(mutateIn, region);
          } else {
            success = checkAndPut(mutateIn, region);
          }
          break;
        case DELETE:
          if (mutateIn.getCondition().getRow() == null) {
            success = simpleDelete(mutateIn, region);
          } else {
            success = checkAndDelete(mutateIn, region);
          }
          break;
        default:
          throw new RuntimeException(
              "mutate supports atomic put and/or delete, not "
                  + type.name()
          );
      }
      mutateResponse = new MutateResponse(null, success);
    } catch (IOException e) {
      mutateResponse = new MutateResponse(null, false);
      LOG.error(e.getLocalizedMessage());
    }

    final Response response = new Response(Response.Command.MUTATE,
        call.getCommandId(),
        null,
        mutateResponse,
        null,
        null);
    ctx.writeAndFlush(response);
  }

  private boolean checkAndDelete(MutateRequest mutateIn, HRegion region) throws IOException {
    boolean success;
    final Condition condition = mutateIn.getCondition();
    final byte[] row = condition.getRow().array();
    final byte[] cf = condition.getFamily().array();
    final byte[] cq = condition.getQualifier().array();

    final CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(condition.getCompareType().name());
    final ByteArrayComparable comparator = ReverseProtobufUtil.toComparator(condition.getComparator());

    success = region.checkAndMutate(row,
        cf,
        cq,
        compareOp,
        comparator,
        ReverseProtobufUtil.toDelete(mutateIn.getMutation()),
        true);
    return success;
  }

  private boolean simpleDelete(MutateRequest mutateIn, HRegion region) {
    try {
      region.delete(ReverseProtobufUtil.toDelete(mutateIn.getMutation()));
    } catch (IOException e) {
      LOG.error(e.getLocalizedMessage());
      return false;
    }
    return true;
  }

  private boolean checkAndPut(MutateRequest mutateIn, HRegion region) throws IOException {
    boolean success;
    final Condition condition = mutateIn.getCondition();
    final byte[] row = condition.getRow().array();
    final byte[] cf = condition.getFamily().array();
    final byte[] cq = condition.getQualifier().array();

    final CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(condition.getCompareType().name());
    final ByteArrayComparable comparator = ReverseProtobufUtil.toComparator(condition.getComparator());

    success = region.checkAndMutate(row,
        cf,
        cq,
        compareOp,
        comparator,
        ReverseProtobufUtil.toPut(mutateIn.getMutation()),
        true);
    return success;
  }

  private boolean simplePut(MutateRequest mutateIn, HRegion region) {
    try {
      region.put(ReverseProtobufUtil.toPut(mutateIn.getMutation()));
    } catch (IOException e) {
      LOG.error(e.getLocalizedMessage());
      return false;
    }
    return true;
  }

  private void scan(ChannelHandlerContext ctx, Call call) throws IOException {
    final ScanRequest scanIn = call.getScan();
    final long scannerId;
    scannerId = getScannerId(scanIn);
    final Integer numberOfRowsToSend = scanIn.getNumberOfRows();
    Channel<Integer> channel = scanManager.getChannel(scannerId);
    // New Scanner
    if (null == channel) {
      final Fiber fiber = new ThreadFiber();
      fiber.start();
      channel = new MemoryChannel<>();
      HRegion region = regionServerService.getOnlineRegion(call.getScan().getRegion());
      if (region == null) {
        throw new IOException("Unable to find region");
      }
      final ScanRunnable scanRunnable = new ScanRunnable(ctx, call, scannerId, region);
      channel.subscribe(fiber, scanRunnable);
      scanManager.addChannel(scannerId, channel);
    }
    channel.publish(numberOfRowsToSend);
  }

  private long getScannerId(ScanRequest scanIn) {
    long scannerId;
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

  private void get(ChannelHandlerContext ctx, Call call) throws IOException {
    final Get getIn = call.getGet().getGet();

    final HRegion region = regionServerService.getOnlineRegion(call.getGet().getRegion());
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(getIn);
    if (region == null) {
      throw new IOException("Unable to find region");
    }
    final Result regionResult = region.get(serverGet);
    final c5db.client.generated.Result result;

    if (getIn.getExistenceOnly()) {
      result = new c5db.client.generated.Result(new ArrayList<>(), 0, regionResult.getExists());
    } else {
      result = ReverseProtobufUtil.toResult(regionResult);
    }
    final GetResponse getResponse = new GetResponse(result);
    final Response response = new Response(Response.Command.GET, call.getCommandId(), getResponse, null, null, null);
    ctx.writeAndFlush(response);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }
}
