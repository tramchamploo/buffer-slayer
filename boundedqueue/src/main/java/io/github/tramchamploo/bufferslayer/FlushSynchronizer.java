package io.github.tramchamploo.bufferslayer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Flush threads should be notified when pending queue has more elements
 * than {@code bufferedMaxMessages} using this flush synchronizer.
 */
class FlushSynchronizer {

  final Queue<AbstractSizeBoundedQueue> queue = new ConcurrentLinkedQueue<>();
  // Threads that waiting to poll block for a certain period of time
  private final ReentrantLock blocker = new ReentrantLock();
  private final Condition notEmpty = blocker.newCondition();

  /**
   * Offer a queue here in order to make flush threads have something to drain
   * <p> if queue already existed, do nothing
   * @param q queue to offer
   */
  boolean offer(AbstractSizeBoundedQueue q) {
    if (q.ready) {
      return false;
    }

    q.ready = true;
    boolean result = queue.offer(q);
    // Not blockingly acquire lock here to ensure fast offering
    if (blocker.tryLock()) {
      try {
        if (blocker.hasWaiters(notEmpty)) {
          notEmpty.signalAll();
        }
      } finally {
        blocker.unlock();
      }
    }
    return result;
  }

  /**
   * Wait until at least one queue appears, then return it.
   * @param timeoutNanos if reaches this timeout and no queue appears, return null
   * @return the first queue offered
   */
  AbstractSizeBoundedQueue poll(long timeoutNanos) {
    boolean wasInterrupted = false;
    AbstractSizeBoundedQueue first;

    blocker.lock();
    try {
      while ((first = queue.poll()) == null) {
        if (timeoutNanos <= 0L)
          return null;

        try {
          timeoutNanos = notEmpty.awaitNanos(timeoutNanos);
        } catch (InterruptedException e) {
          wasInterrupted = true;
        }
      }
    } finally {
      blocker.unlock();
    }

    // recover interrupt status
    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }

    return first;
  }

  /**
   * Remove the key from the map so it can be offered again
   */
  void release(AbstractSizeBoundedQueue q) {
    q.ready = false;
  }

  /**
   * Clear everything
   */
  void clear() {
    queue.clear();
  }
}
