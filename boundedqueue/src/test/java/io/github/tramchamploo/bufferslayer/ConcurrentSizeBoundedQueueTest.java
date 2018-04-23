package io.github.tramchamploo.bufferslayer;

public final class ConcurrentSizeBoundedQueueTest extends AbstractSizeBoundedQueueTest {

  protected AbstractSizeBoundedQueue newQueue(OverflowStrategy.Strategy strategy) {
    return new ConcurrentSizeBoundedQueue(16, strategy);
  }
}
