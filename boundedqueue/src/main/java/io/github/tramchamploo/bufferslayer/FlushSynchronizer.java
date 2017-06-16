package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by tramchamploo on 2017/5/25.
 * <p>Flush threads should block when no pending queue has more elements
 * than {@code bufferedMaxMessages}.
 */
class FlushSynchronizer {

  private final Lock lock = new ReentrantLock();
  private final Condition available = lock.newCondition();
  // Use a HashMap to faster the existence check of queues
  final Map<MessageKey, Object> keyToReady = new HashMap<>();
  private static final Object READY = new Object();

  ArrayDeque<SizeBoundedQueue> deque = new ArrayDeque<>();

  /**
   * Offer a queue here in order to make flush threads have something to drain
   * <p> if queue already existed, do nothing
   * @param q queue to offer
   */
  boolean offer(SizeBoundedQueue q) {
    lock.lock();
    try {
      if (!keyToReady.containsKey(q.key)) {
        keyToReady.put(q.key, READY);
        boolean result = deque.offer(q);
        available.signal();
        return result;
      }
    } finally {
      lock.unlock();
    }
    return false;
  }

  /**
   * Wait until at least one queue appears, then return it.
   * @param timeoutNanos if reaches this timeout and no queue appears, return null
   * @return the first queue offered
   */
  SizeBoundedQueue poll(long timeoutNanos) throws InterruptedException {
    lock.lock();
    try {
      long nanosLeft = timeoutNanos;
      while (deque.isEmpty()) {
        if (nanosLeft <= 0) return null;
        nanosLeft = available.awaitNanos(nanosLeft);
      }
      return deque.poll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove the key from the map so it can be offered again
   */
  void release(SizeBoundedQueue q) {
    lock.lock();
    try {
      keyToReady.remove(q.key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Clear everything
   */
  void clear() {
    lock.lock();
    try {
      keyToReady.clear();
      deque.clear();
    } finally {
      lock.unlock();
    }
  }
}
