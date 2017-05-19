package io.bufferslayer;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by tramchamploo on 2017/5/4.
 */
final class RoundRobinQueueRecycler extends AbstractQueueRecycler {

  final LinkedBlockingQueue<SizeBoundedQueue> roundRobin;

  RoundRobinQueueRecycler(int pendingMaxMessages, Strategy overflowStrategy,
      long pendingKeepaliveNanos) {
    super(pendingMaxMessages, overflowStrategy, pendingKeepaliveNanos);
    this.roundRobin = new LinkedBlockingQueue<>();
  }

  @Override
  BlockingQueue<SizeBoundedQueue> recycler() {
    return roundRobin;
  }
}
