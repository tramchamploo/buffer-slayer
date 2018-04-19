package io.github.tramchamploo.bufferslayer;

public final class BlockingSizeBoundedQueueTest extends AbstractSizeBoundedQueueTest {

  protected AbstractSizeBoundedQueue newQueue(OverflowStrategy.Strategy strategy) {
    return new BlockingSizeBoundedQueue(16, strategy);
  }
}
