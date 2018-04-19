package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;

/**
 * Factory for {@link ConcurrentSizeBoundedQueue}
 */
public final class ConcurrentSizeBoundedQueueFactory extends SizeBoundedQueueFactory {

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  protected int priority() {
    return 10;
  }

  @Override
  protected AbstractSizeBoundedQueue newQueue(int maxSize, Strategy overflowStrategy,
      MessageKey key) {
    return new ConcurrentSizeBoundedQueue(maxSize, overflowStrategy, key);
  }
}
