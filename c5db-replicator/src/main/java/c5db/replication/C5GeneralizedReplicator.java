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
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.ReplicateSubmissionInfo;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.replication.ReplicatorReceipt;
import c5db.util.C5Futures;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.Nullable;
import org.jetlang.channels.ChannelSubscription;
import org.jetlang.fibers.Fiber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

/**
 * A GeneralizedReplicator that makes use of a {@link c5db.interfaces.replication.Replicator},
 * processing its ReplicatorReceipts and IndexCommitNotices to provide a more general-purpose
 * interface.
 */
public class C5GeneralizedReplicator implements GeneralizedReplicator {
  private final long nodeId;
  private final Replicator replicator;
  private final Fiber fiber;

  private SettableFuture<Void> availableFuture;

  /**
   * Queue of receipts for pending log requests and their futures; access this queue only
   * from the fiber. See {@link c5db.replication.C5GeneralizedReplicator.ReceiptWithCompletionFuture}
   */
  private final Queue<ReceiptWithCompletionFuture> receiptQueue =
      new ArrayDeque<>(ReplicatorConstants.REPLICATOR_MAXIMUM_SIMULTANEOUS_LOG_REQUESTS);

  /**
   * Both the fiber and replicator must be started by the user of this class, and the
   * user takes responsibility for their disposal.
   */
  public C5GeneralizedReplicator(Replicator replicator, Fiber fiber) {
    this.nodeId = replicator.getId();
    this.replicator = replicator;
    this.fiber = fiber;

    setupCommitNoticeSubscription();
    setupEventNoticeSubscription();
  }

  @Override
  public ListenableFuture<ReplicateSubmissionInfo> replicate(List<ByteBuffer> data) throws InterruptedException,
      InvalidReplicatorStateException {

    final ReceiptWithCompletionFuture receiptWithCompletionFuture =
        new ReceiptWithCompletionFuture(replicator.logData(data));

    if (receiptWithCompletionFuture.receiptFuture == null) {
      throw new InvalidReplicatorStateException("Replicator is not in the leader state");
    }

    // Add to the queue on the fiber for concurrency safety
    fiber.execute(
        () -> receiptQueue.add(receiptWithCompletionFuture));

    return Futures.transform(receiptWithCompletionFuture.receiptFuture,
        (ReplicatorReceipt receipt) ->
            new ReplicateSubmissionInfo(receipt.seqNum, receiptWithCompletionFuture.completionFuture));
  }

  @Override
  public ListenableFuture<Void> isAvailableFuture() {
    SettableFuture<Void> returnedFuture = SettableFuture.create();

    fiber.execute(() -> {
      if (this.availableFuture == null) {
        // By placing this future here, the handleEventNotice method will know someone is waiting
        // for notification of availability; that method will handle setting this.
        this.availableFuture = returnedFuture;

      } else {
        // Some other invocation of this method is already waiting for notification of availability,
        // so setup that existing availableFuture to "forward" its result to the newly created one.
        C5Futures.addCallback(this.availableFuture, returnedFuture::set, returnedFuture::setException, fiber);
      }
    });

    return returnedFuture;
  }

  private void setupCommitNoticeSubscription() {
    final String quorumId = replicator.getQuorumId();
    final long serverNodeId = replicator.getId();

    replicator.getCommitNoticeChannel().subscribe(
        new ChannelSubscription<>(this.fiber, this::handleCommitNotice,
            (notice) ->
                notice.nodeId == serverNodeId
                    && notice.quorumId.equals(quorumId)));
  }

  private void setupEventNoticeSubscription() {
    replicator.getEventChannel().subscribe(fiber, this::handleEventNotice);
  }

  /**
   * The core of the logic is in this method. When we receive an IndexCommitNotice, we need
   * to find out which, if any, of the pending replicate requests are affected by it. We do
   * that by examining their receipts, if the receipts are available.
   *
   * @param notice An IndexCommitNotice received from the internal Replicator.
   */
  @FiberOnly
  private void handleCommitNotice(IndexCommitNotice notice) {
    while (!receiptQueue.isEmpty() && receiptQueue.peek().receiptFuture.isDone()) {
      ReceiptWithCompletionFuture receiptWithCompletionFuture = receiptQueue.peek();
      SettableFuture<Void> completionFuture = receiptWithCompletionFuture.completionFuture;

      ReplicatorReceipt receipt = getReceiptOrSetException(receiptWithCompletionFuture);

      if (receipt != null) {
        if (notice.lastIndex < receipt.seqNum) {
          // old commit notice
          return;

        } else if (receipt.seqNum < notice.firstIndex) {
          completionFuture.setException(new IOException("commit notice skipped over the receipt's seqNum"));

        } else if (notice.term != receipt.term) {
          completionFuture.setException(new IOException("commit notice's term differs from that of receipt"));

        } else {
          // receipt.seqNum is within the range of the commit notice, and the terms match: replication is complete
          completionFuture.set(null);
        }
      }

      receiptQueue.poll();
    }
  }

  /**
   * This method runs whenever our wrapped Replicator emits an event notice. It checks to
   * see if the replicator is announcing that it has been elected leader of its quorum. If so,
   * then this GeneralizedReplicator is now available for submission of replication requests.
   * Therefore, check if anyone has been waiting to be notified of our availability, by having
   * left an unset availableFuture. If so, notify them by setting that pending future..
   */
  @FiberOnly
  private void handleEventNotice(ReplicatorInstanceEvent eventNotice) {
    if (availableFuture != null
        && eventNotice.instance == replicator
        && eventNotice.eventType == ReplicatorInstanceEvent.EventType.LEADER_ELECTED
        && eventNotice.newLeader == nodeId) {

      // Notify past callers of isAvailableFuture
      availableFuture.set(null);

      // Remove the future so that a subsequent invocation of this method will not try
      // to set it again.
      availableFuture = null;
    }
  }

  /**
   * This method assumes that the receiptFuture is "done". If the future is set with a value,
   * return its value, the ReplicatorReceipt. On the other hand, if it is an exception, set
   * the completionFuture with that exception and return null. A null return value guarantees
   * the completionFuture has been set with an exception.
   */
  @Nullable
  private ReplicatorReceipt getReceiptOrSetException(ReceiptWithCompletionFuture receiptWithCompletionFuture) {
    ListenableFuture<ReplicatorReceipt> receiptFuture = receiptWithCompletionFuture.receiptFuture;
    SettableFuture<?> completionFuture = receiptWithCompletionFuture.completionFuture;

    assert receiptFuture.isDone();

    try {
      ReplicatorReceipt receipt = C5Futures.getUninterruptibly(receiptFuture);
      if (receipt == null) {
        completionFuture.setException(new IOException("replicator returned a null receipt"));
      }
      return receipt;
    } catch (ExecutionException e) {
      completionFuture.setException(e);
      return null;
    }
  }

  /**
   * C5GeneralizedReplicator keeps track of all the times it's requested to replicate
   * data. Each of these replication requests, to its internal replicator, yields a
   * receipt. Later on, the internal replicator will issue a commit notice, indicating
   * that one or more earlier requests have been completed. The receipts are bundled
   * together with SettableFutures so that the user of this object can be notified
   * when that happens. ReceiptWithCompletionFuture is the class used to bundle the receipt and
   * the completion future together.
   */
  private static class ReceiptWithCompletionFuture {
    public final ListenableFuture<ReplicatorReceipt> receiptFuture;
    public final SettableFuture<Void> completionFuture = SettableFuture.create();

    private ReceiptWithCompletionFuture(ListenableFuture<ReplicatorReceipt> receiptFuture) {
      this.receiptFuture = receiptFuture;
    }
  }
}
