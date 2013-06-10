package ohmdb.regionserver;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import ohmdb.client.generated.ClientProtos;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OhmServerHandler extends
    ChannelInboundMessageHandlerAdapter<ClientProtos.Call> {

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
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
      HRegion region = OhmServer.getOnlineRegion("1");
      region.mutateRow(rm);
    }
    ClientProtos.Response response = ClientProtos
        .Response
        .newBuilder()
        .setCommand(ClientProtos.Response.Command.MULTI)
        .setCommandId(call.getCommandId())
        .setMulti(multiResponse.build()).build();
    ctx.write(response);
  }


  private void mutate(ChannelHandlerContext ctx, ClientProtos.Call call)
      throws IOException {
    ClientProtos.MutateRequest mutateIn = call.getMutate();
    ClientProtos.MutateResponse.Builder mutateResponse =
        ClientProtos.MutateResponse.newBuilder();
    try {
      HRegion region = OhmServer.getOnlineRegion("1");
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
    ctx.write(response);
  }


  private void multiGet(ChannelHandlerContext ctx, ClientProtos.Call call)
      throws IOException {
    ClientProtos.MultiGetResponse.Builder getResponse =
        ClientProtos.MultiGetResponse.newBuilder();
    HRegion region = OhmServer.getOnlineRegion("1");
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
    ctx.write(response);

  }

  private void scan(ChannelHandlerContext ctx, ClientProtos.Call call)
      throws IOException {
    ClientProtos.Scan scanIn = call.getScan().getScan();
    HRegion region = OhmServer.getOnlineRegion("1");
    InternalScanner scanner =
        region.getScanner(ReverseProtobufUtil.toScan(scanIn));
    boolean moreResults;
    ClientProtos.ScanResponse.Builder scanResponse
        = ClientProtos.ScanResponse.newBuilder();
    ClientProtos.Result.Builder resultBuilder =
        ClientProtos.Result.newBuilder();


    // This should go up by ^7
    int result_climber = 100;
    do {
      List<KeyValue> kvs = new ArrayList<>();
      moreResults = scanner.next(kvs, result_climber);
      byte[] previousRowKey = null;
      Iterator<KeyValue> kvIt = kvs.iterator();

      while (kvIt.hasNext()) {
        KeyValue kv = kvIt.next();
        byte[] rowKey = kv.getRow();
        resultBuilder.addCell(ReverseProtobufUtil.toCell(kv));

        // If we have a whole row set it up as a result
        if ((previousRowKey != rowKey && previousRowKey != null)
            || !kvIt.hasNext()) {
          scanResponse.addResult(resultBuilder.build());
          resultBuilder = ClientProtos.Result.newBuilder();
        }
        previousRowKey = rowKey;
      }
      scanResponse.setMoreResults(true);
      ClientProtos.Response response = ClientProtos
          .Response
          .newBuilder()
          .setCommand(ClientProtos.Response.Command.SCAN)
          .setCommandId(call.getCommandId())
          .setScan(scanResponse.build()).build();

      ctx.write(response);
      scanResponse.clear();
      if (result_climber < 10000) {
        result_climber = result_climber * result_climber;
      }
    } while (moreResults);

    scanResponse.setMoreResults(false);
    ClientProtos.Response response = ClientProtos
        .Response
        .newBuilder()
        .setCommand(ClientProtos.Response.Command.SCAN)
        .setCommandId(call.getCommandId())
        .setScan(scanResponse.build()).build();
    ctx.write(response);
    scanner.close();
  }

  private void get(ChannelHandlerContext ctx, ClientProtos.Call call)
      throws IOException {
    ClientProtos.Get getIn = call.getGet().getGet();
    ClientProtos.GetResponse.Builder getResponse =
        ClientProtos.GetResponse.newBuilder();
    HRegion region = OhmServer.getOnlineRegion("1");
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
    ctx.write(response);
  }
}


