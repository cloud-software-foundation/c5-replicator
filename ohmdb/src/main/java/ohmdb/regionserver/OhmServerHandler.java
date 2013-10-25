package ohmdb.regionserver;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import ohmdb.interfaces.OhmServer;
import ohmdb.OhmStatic;
import ohmdb.client.generated.ClientProtos;
import ohmdb.regionserver.scanner.ScanRunnable;
import ohmdb.regionserver.scanner.ScannerManager;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ohmdb.OhmStatic.*;

public class OhmServerHandler extends
    SimpleChannelInboundHandler<ClientProtos.Call> {
  ScannerManager scanManager = ScannerManager.INSTANCE;

  @Override
  public void channelRead0(final ChannelHandlerContext ctx,
                           final ClientProtos.Call call)
      throws Exception {
    switch (call.getCommand()) {
      case GET:
        get(ctx, call);
        break;
      case MULTI_GET:
        multiGet(ctx, call);
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
    List<ClientProtos.MutationProto> mutations =
        new ArrayList<>(request.getActionCount());

    for (ClientProtos.MultiAction actionUnion : request.getActionList()) {
      if (actionUnion.hasMutation()) {
        mutations.add(actionUnion.getMutation());
      } else {
        throw new IOException("Unsupported atomic action type: " + actionUnion);
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
            throw new DoNotRetryIOException(
                "mutate supports atomic put and/or delete, not "
                    + type.name());
        }
      }
      HRegion region = getOnlineRegion("1");
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
      HRegion region = getOnlineRegion("1");
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


  private void multiGet(ChannelHandlerContext ctx, ClientProtos.Call call)
      throws IOException {
    ClientProtos.MultiGetResponse.Builder getResponse =
        ClientProtos.MultiGetResponse.newBuilder();
    HRegion region = getOnlineRegion("1");
    for (ClientProtos.Get get : call.getMultiGet().getGetList()) {
      Result result = region.get(ReverseProtobufUtil.toGet(get));
      getResponse.addExists(result != null && !result.isEmpty());

      if (!call.getGet().getExistenceOnly()) {
        getResponse.addResult(ReverseProtobufUtil.toResult(result));
      }
    }

    ClientProtos.Response response = ClientProtos
        .Response
        .newBuilder()
        .setCommand(ClientProtos.Response.Command.MULTI_GET)
        .setCommandId(call.getCommandId())
        .setMultiGet(getResponse.build()).build();
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

      ScanRunnable scanRunnable = new ScanRunnable(ctx, call, scannerId);
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
    HRegion region = getOnlineRegion("1");
    Result result = region.get(ReverseProtobufUtil.toGet(getIn));
    getResponse.setExists(result != null && !result.isEmpty());
    if (!call.getGet().getExistenceOnly()) {
      getResponse.setResult(ReverseProtobufUtil.toResult(result));
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


