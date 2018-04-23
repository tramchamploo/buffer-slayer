package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;

/**
 * Factory for {@link BlockingSizeBoundedQueue}
 */
public final class BlockingSizeBoundedQueueFactory extends SizeBoundedQueueFactory {

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  protected int priority() {
    return 5;
  }

  @Override
  protected AbstractSizeBoundedQueue newQueue(int maxSize, Strategy overflowStrategy,
      MessageKey key) {
    return new BlockingSizeBoundedQueue(maxSize, overflowStrategy, key);
  }
}
