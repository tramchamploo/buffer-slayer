package io.bufferslayer;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by tramchamploo on 2017/5/4.
 */
final class SizeFirstQueueRecycler extends AbstractQueueRecycler {

  final PriorityBlockingQueue<SizeBoundedQueue> sizeFirst;

  SizeFirstQueueRecycler(int pendingMaxMessages, Strategy overflowStrategy,
      long pendingKeepaliveNanos) {
    super(pendingMaxMessages, overflowStrategy, pendingKeepaliveNanos);

    Comparator<SizeBoundedQueue> queueComparator = new Comparator<SizeBoundedQueue>() {
      @Override
      public int compare(SizeBoundedQueue q1, SizeBoundedQueue q2) {
        return q2.count - q1.count;
      }
    };
    this.sizeFirst = new PriorityBlockingQueue<>(16, queueComparator);
  }

  @Override
  BlockingQueue<SizeBoundedQueue> recycler() {
    return sizeFirst;
  }
}
