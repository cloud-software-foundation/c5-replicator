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

package c5db.replication;

import c5db.ReplicatorConstants;
import c5db.codec.ProtostuffDecoder;
import c5db.codec.ProtostuffEncoder;
import c5db.interfaces.C5Module;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.LogModule;
import c5db.interfaces.ModuleServer;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.discovery.NodeInfoRequest;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.messages.generated.ModuleType;
import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import c5db.util.C5Futures;
import c5db.util.FiberOnly;
import c5db.util.FiberSupplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.channels.Session;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * An implementation of ReplicationModule using instances of ReplicatorInstance to handle each quorum.
 * <p>
 * TODO consider being symmetric in how we handle sent messages.
 */
public class ReplicatorService extends AbstractService implements ReplicationModule {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicatorService.class);

  /**
   * ************* C5Module informational methods ***********************************
   */

  @Override
  public ModuleType getModuleType() {
    return ModuleType.Replication;
  }

  @Override
  public boolean hasPort() {
    return true;
  }

  @Override
  public int port() {
    return this.port;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }

  @Override
  public ListenableFuture<Replicator> createReplicator(final String quorumId,
                                                       final Collection<Long> peers) {
    final SettableFuture<Replicator> future = SettableFuture.create();

    ListenableFuture<ReplicatorLog> logFuture = logModule.getReplicatorLog(quorumId);

    C5Futures.addCallback(logFuture,
        (ReplicatorLog log) -> {
          Replicator replicator = createReplicatorWithLog(log, quorumId, peers);
          future.set(replicator);
        },
        future::setException,
        fiber
    );

    return future;
  }

  private final int port;
  private final ModuleServer moduleServer;
  private final FiberSupplier fiberSupplier;
  private final long nodeId;

  // Netty infrastructure
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ChannelGroup allChannels;
  private final ServerBootstrap serverBootstrap = new ServerBootstrap();
  private final Bootstrap outgoingBootstrap = new Bootstrap();

  // ReplicatorInstances and objects shared among them
  private final Map<String, ReplicatorInstance> replicatorInstances = new HashMap<>();
  private final Persister persister;
  private final RequestChannel<RpcRequest, RpcWireReply> outgoingRequests = new MemoryRequestChannel<>();
  private final MemoryChannel<ReplicatorInstanceEvent> replicatorEventChannel = new MemoryChannel<>();
  private final MemoryChannel<IndexCommitNotice> indexCommitNotices = new MemoryChannel<>();

  // Connections to other servers by their node IDs
  private final Map<Long, Channel> connections = new HashMap<>();

  // Map of message ID -> Request
  // TODO we need a way to remove these after a while, because if we fail to get a reply we will be unhappy.
  private final Map<Long, Request<RpcRequest, RpcWireReply>> outstandingRPCs = new HashMap<>();

  // Map of Session -> message ID
  private final Map<Session, Long> outstandingRPCbySession = new HashMap<>();

  // Initialized in the module start, by the time any messages or fiber executions trigger, these should be not-null
  private DiscoveryModule discoveryModule = null;
  private LogModule logModule = null;
  private Channel listenChannel;
  private Fiber fiber;

  // Sequence number for sent messages
  private long messageIdGen = 1;

  /**
   * ReplicatorService creates and starts fibers; it must be stopped (or failed) in
   * order to dispose them.
   */
  public ReplicatorService(EventLoopGroup bossGroup,
                           EventLoopGroup workerGroup,
                           long nodeId,
                           int port,
                           ModuleServer moduleServer,
                           FiberSupplier fiberSupplier,
                           QuorumFileReaderWriter quorumFileReaderWriter) {
    this.bossGroup = bossGroup;
    this.workerGroup = workerGroup;
    this.nodeId = nodeId;
    this.port = port;
    this.moduleServer = moduleServer;
    this.fiberSupplier = fiberSupplier;

    this.allChannels = new DefaultChannelGroup(workerGroup.next());
    this.persister = new Persister(quorumFileReaderWriter);
  }

  /**
   * *************** Handlers for netty/messages from the wire/TCP ***********************
   */
  @ChannelHandler.Sharable
  private class MessageHandler extends SimpleChannelInboundHandler<ReplicationWireMessage> {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());

      super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ReplicationWireMessage msg) throws Exception {
      fiber.execute(() -> handleWireInboundMessage(ctx.channel(), msg));
    }
  }

  @FiberOnly
  private void handleWireInboundMessage(Channel channel, ReplicationWireMessage msg) {
    long messageId = msg.getMessageId();
    if (msg.getReceiverId() != nodeId) {
      LOG.debug("Got messageId {} for {} but I am {}, ignoring!", messageId, msg.getReceiverId(), nodeId);
      return;
    }

    if (msg.getInReply()) {
      Request<RpcRequest, RpcWireReply> request = outstandingRPCs.get(messageId);
      if (request == null) {
        LOG.debug("Got a reply message_id {} which we don't track", messageId);
        return;
      }

      outstandingRPCs.remove(messageId);
      outstandingRPCbySession.remove(request.getSession());
      request.reply(new RpcWireReply(msg));
    } else {
      handleWireRequestMessage(channel, msg);
    }
  }

  @FiberOnly
  private void handleWireRequestMessage(final Channel channel, final ReplicationWireMessage msg) {
    RpcWireRequest wireRequest = new RpcWireRequest(msg);
    String quorumId = wireRequest.quorumId;

    ReplicatorInstance replInst = replicatorInstances.get(quorumId);
    if (replInst == null) {
      LOG.trace("Instance not found {} for message id {} from {} (normal during region bootstrap)",
          quorumId,
          msg.getMessageId(),
          msg.getSenderId());
      // TODO send RPC failure to the sender?
      return;
    }

    AsyncRequest.withOneReply(fiber, replInst.getIncomingChannel(), wireRequest, reply -> {
      if (!channel.isOpen()) {
        // TODO cant signal comms failure, so just drop on the floor. Is there a better thing to do?
        return;
      }

      ReplicationWireMessage b = reply.getWireMessage(
          msg.getMessageId(),
          nodeId,
          msg.getSenderId(),
          true
      );

      channel.writeAndFlush(b).addListener(
          future -> {
            if (!future.isSuccess()) {
              LOG.warn("node {} error sending reply {} to node {} in response to request {}: {}",
                  nodeId, reply, wireRequest.from, wireRequest, future.cause());
            }
          });
    });
  }

  /**
   * ************* Handlers for Request<> from replicator instances ***********************************
   */
  @FiberOnly
  private void handleCancelledSession(Session session) {
    Long messageId = outstandingRPCbySession.get(session);
    outstandingRPCbySession.remove(session);
    if (messageId == null) {
      return;
    }
    LOG.trace("Removing cancelled RPC, message ID {}", messageId);
    outstandingRPCs.remove(messageId);
  }

  @FiberOnly
  private void handleOutgoingMessage(final Request<RpcRequest, RpcWireReply> message) {
    final RpcRequest request = message.getRequest();
    final long to = request.to;

    if (to == nodeId) {
      handleLoopBackMessage(message);
      return;
    }

    // check to see if we have a connection:
    Channel channel = connections.get(to);
    if (channel != null && channel.isOpen()) {
      sendMessageAsync(message, channel);
      return;
    } else if (channel != null) {
      // stale?
      LOG.debug("Removing stale !isOpen channel from connections.get() for peer {}", to);
      connections.remove(to);
    }

    NodeInfoRequest nodeInfoRequest = new NodeInfoRequest(to, ModuleType.Replication);
    LOG.debug("node {} sending node info request {} ", nodeId, nodeInfoRequest);
    AsyncRequest.withOneReply(fiber, discoveryModule.getNodeInfo(), nodeInfoRequest, new Callback<NodeInfoReply>() {
      @SuppressWarnings("RedundantCast")
      @FiberOnly
      @Override
      public void onMessage(NodeInfoReply nodeInfoReply) {
        if (!nodeInfoReply.found) {
          LOG.debug("Can't find the info for the peer {}", to);
          // TODO signal TCP/transport layer failure in a better way
          //message.reply(null);
          return;
        }

        LOG.debug("node {} got node info for node {} reply {} ", nodeId, to, nodeInfoReply);
        // what if existing outgoing connection attempt?
        Channel channel = connections.get(to);
        if (channel != null && channel.isOpen()) {
          sendMessageAsync(message, channel);
          return;
        } else if (channel != null) {
          LOG.debug("Removing stale2 !isOpen channel from connections.get() for peer {}", to);
          connections.remove(to);
        }

        // ok so we connect now:
        ChannelFuture channelFuture = outgoingBootstrap.connect(nodeInfoReply.addresses.get(0), nodeInfoReply.port);
        LOG.trace("Connecting to peer {} at address {} port {}", to, nodeInfoReply.addresses.get(0), nodeInfoReply.port);

        // the channel might not be open, so defer the write.
        connections.put(to, channelFuture.channel());
        channelFuture.channel().closeFuture().addListener((ChannelFutureListener)
            future ->
                fiber.execute(() -> {
                  // remove only THIS channel. It might have been removed prior so.
                  connections.remove(to, future.channel());
                }));

        // funny hack, if the channel future is already open, we execute immediately!
        channelFuture.addListener((ChannelFutureListener)
            future -> {
              if (future.isSuccess()) {
                sendMessageAsync(message, future.channel());
              }
            });
      }
    },
        // If the NodeInfoRequest times out:
        ReplicatorConstants.REPLICATOR_NODE_INFO_REQUEST_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS,
        () -> LOG.warn("node info request timeout {} ", nodeInfoRequest));
  }

  private void sendMessageAsync(final Request<RpcRequest, RpcWireReply> message, final Channel channel) {
    fiber.execute(() -> {
      RpcRequest request = message.getRequest();
      long to = request.to;
      long messageId = messageIdGen++;

      outstandingRPCs.put(messageId, message);
      outstandingRPCbySession.put(message.getSession(), messageId);

      LOG.trace("Sending message id {} to {} / {}", messageId, to, request.quorumId);

      ReplicationWireMessage wireMessage = request.getWireMessage(
          messageId,
          nodeId,
          to,
          false
      );

      channel.writeAndFlush(wireMessage).addListener(
          future -> {
            if (!future.isSuccess()) {
              LOG.warn("Error sending from node {} request {}: {}", nodeId, request, future.cause());
            }
          });
    });
  }

  private void handleLoopBackMessage(final Request<RpcRequest, RpcWireReply> origMessage) {
    final long toFrom = nodeId; // I am me.
    final RpcRequest request = origMessage.getRequest();
    final String quorumId = request.quorumId;

    // Funny thing we don't have a direct handle on who sent us this message, so we have to do this. Sok though.
    final ReplicatorInstance repl = replicatorInstances.get(quorumId);
    if (repl == null) {
      // rare failure condition, whereby the replicator died AFTER it send messages.
      return; // ignore the message.
    }

    final RpcWireRequest newRequest = new RpcWireRequest(toFrom, quorumId, request.message);
    AsyncRequest.withOneReply(fiber, repl.getIncomingChannel(), newRequest, msg -> {
      assert msg.message != null;
      RpcWireReply newReply = new RpcWireReply(toFrom, toFrom, quorumId, msg.message);
      origMessage.reply(newReply);
    });
  }


  /**
   * ********** Service startup/registration and shutdown/termination **************
   */
  @Override
  protected void doStart() {
    // must start the fiber up early.
    fiber = fiberSupplier.getFiber(this::failModule);
    setupEventChannelSubscription();
    fiber.start();

    C5Futures.addCallback(getDependedOnModules(),
        (ignore) -> {
          ChannelInitializer<SocketChannel> initer = new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline p = ch.pipeline();
              p.addLast("frameDecode", new ProtobufVarint32FrameDecoder());
              p.addLast("pbufDecode", new ProtostuffDecoder<>(ReplicationWireMessage.getSchema()));

              p.addLast("frameEncode", new ProtobufVarint32LengthFieldPrepender());
              p.addLast("pbufEncoder", new ProtostuffEncoder<ReplicationWireMessage>());

              p.addLast(new MessageHandler());
            }
          };

          serverBootstrap.group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .option(ChannelOption.SO_REUSEADDR, true)
              .option(ChannelOption.SO_BACKLOG, 100)
              .childOption(ChannelOption.TCP_NODELAY, true)
              .childHandler(initer);

          //noinspection RedundantCast
          serverBootstrap.bind(port).addListener((ChannelFutureListener)
              future -> {
                if (future.isSuccess()) {
                  LOG.info("successfully bound node {} port {} ", nodeId, port);
                  listenChannel = future.channel();
                } else {
                  LOG.error("Unable to bind! ", future.cause());
                  failModule(future.cause());
                }
              });

          outgoingBootstrap.group(workerGroup)
              .channel(NioSocketChannel.class)
              .option(ChannelOption.SO_REUSEADDR, true)
              .option(ChannelOption.TCP_NODELAY, true)
              .handler(initer);

          //noinspection Convert2MethodRef
          outgoingRequests.subscribe(fiber, message -> handleOutgoingMessage(message),
              // Clean up cancelled requests.
              message -> handleCancelledSession(message.getSession())
          );

          notifyStarted();

        },
        (Throwable t) -> {
          LOG.error("ReplicatorService unable to retrieve modules!", t);
          failModule(t);
        }, fiber);
  }

  protected void failModule(Throwable t) {
    LOG.error("ReplicatorService failure, shutting down all ReplicatorInstances", t);
    try {
      replicatorInstances.values().forEach(ReplicatorInstance::dispose);
      replicatorInstances.clear();
      fiber.dispose();
      if (listenChannel != null) {
        listenChannel.close();
      }
      allChannels.close();
    } finally {
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    fiber.execute(() -> {
      final AtomicInteger countDown = new AtomicInteger(1);
      GenericFutureListener<? extends Future<? super Void>> listener = future -> {
        if (countDown.decrementAndGet() == 0) {
          notifyStopped();
        }

      };
      if (listenChannel != null) {
        countDown.incrementAndGet();
        listenChannel.close().addListener(listener);
      }

      allChannels.close().addListener(listener);
      replicatorInstances.values().forEach(ReplicatorInstance::dispose);
      replicatorInstances.clear();
      fiber.dispose();
      fiber = null;
    });
  }

  private void setupEventChannelSubscription() {
    replicatorEventChannel.subscribe(fiber, message -> {
      if (message.eventType == ReplicatorInstanceEvent.EventType.QUORUM_FAILURE) {
        LOG.error("replicator {} indicates failure, removing. Error {}", message.instance,
            message.error);
        replicatorInstances.remove(message.instance.getQuorumId());
      } else {
        LOG.debug("replicator indicates state change {}", message);
      }
    });
  }

  private ListenableFuture<Void> getDependedOnModules() {
    SettableFuture<Void> doneFuture = SettableFuture.create();

    List<ListenableFuture<C5Module>> moduleFutures = new ArrayList<>();
    moduleFutures.add(moduleServer.getModule(ModuleType.Log));
    moduleFutures.add(moduleServer.getModule(ModuleType.Discovery));

    ListenableFuture<List<C5Module>> compositeModulesFuture = Futures.allAsList(moduleFutures);

    LOG.warn("ReplicatorService now waiting for module dependency on Log & Discovery");

    C5Futures.addCallback(compositeModulesFuture,
        (List<C5Module> modules) -> {
          this.logModule = (LogModule) modules.get(0);
          this.discoveryModule = (DiscoveryModule) modules.get(1);

          doneFuture.set(null);
        },
        this::failModule, fiber);

    return doneFuture;
  }

  private Replicator createReplicatorWithLog(ReplicatorLog log, String quorumId, Collection<Long> peers) {
    if (replicatorInstances.containsKey(quorumId)) {
      LOG.debug("Replicator for quorum {} exists already", quorumId);
      return replicatorInstances.get(quorumId);
    }

    if (!peers.contains(nodeId)) {
      LOG.warn("Creating a replicator instance for quorum {} peers {} but it does not contain me ({})",
          quorumId, peers, nodeId);
    }

    LOG.info("Creating replicator instance for {} peers {}", quorumId, peers);

    MemoryChannel<Throwable> throwableChannel = new MemoryChannel<>();
    Fiber instanceFiber = fiberSupplier.getFiber(throwableChannel::publish);
    ReplicatorInstance instance =
        new ReplicatorInstance(
            instanceFiber,
            nodeId,
            quorumId,
            log,
            new DefaultSystemTimeReplicatorClock(),
            persister,
            outgoingRequests,
            replicatorEventChannel,
            indexCommitNotices,
            Replicator.State.FOLLOWER
        );
    if (log.getLastIndex() == 0) {
      instance.bootstrapQuorum(peers);
    }
    throwableChannel.subscribe(fiber, instance::failReplicatorInstance);
    replicatorInstances.put(quorumId, instance);
    return instance;
  }
}
