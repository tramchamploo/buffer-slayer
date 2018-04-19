package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;

/**
 * A thread-safe queue that could be blocking or non-blocking
 *
 * @see BlockingSizeBoundedQueue
 * @see ConcurrentSizeBoundedQueue
 */
abstract class AbstractSizeBoundedQueue {

  interface Consumer {

    /**
     * Returns true if it accepted the next element
     */
    boolean accept(MessagePromise<?> next);
  }

  // Flag indicates if this queue's size exceeds bufferedMaxMessages and ready to be drained
  volatile boolean ready = false;

  private volatile long lastAccessNanos = System.nanoTime();

  final int maxSize;
  final MessageKey key;
  // Set promise success if true, only used in benchmark
  boolean _benchmark = false;

  AbstractSizeBoundedQueue(int maxSize, MessageKey key) {
    this.maxSize = maxSize;
    this.key = key;
  }

  /**
   * Notify deferred if the element could be added or reject if it could not due to its
   * {@link OverflowStrategy.Strategy}.
   */
  abstract void offer(MessagePromise<?> promise);

  /**
   * Drain all elements of this queue to a consumer
   */
  abstract int drainTo(Consumer consumer);

  /**
   * Clears the queue unconditionally and returns count of elements cleared.
   */
  abstract int clear();

  /**
   * Return the size of this queue
   */
  abstract int size();

  /**
   * Return {@code true} if empty else {@code false}
   */
  abstract boolean isEmpty();

  /**
   * set last access time to now
   */
  void recordAccess() {
    if (key != Message.SINGLE_KEY) {
      lastAccessNanos = System.nanoTime();
    }
  }

  /**
   * last time of this key accessed in nanoseconds
   */
  long lastAccessNanos() {
    return key == Message.SINGLE_KEY ? Long.MAX_VALUE : lastAccessNanos;
  }

  /**
   * Set the benchmark mode
   */
  void _setBenchmarkMode(boolean benchmark) {
    _benchmark = benchmark;
  }
}
