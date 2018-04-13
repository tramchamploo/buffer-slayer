package io.github.tramchamploo.bufferslayer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * <p>Flush threads should be notified when pending queue has more elements
 * than {@code bufferedMaxMessages} using this flush synchronizer.
 */
class FlushSynchronizer {

  final Queue<SizeBoundedQueue> queue = new ConcurrentLinkedQueue<>();
  // Threads that waiting to poll
  private final Queue<Thread> waiters = new ConcurrentLinkedQueue<>();

  /**
   * Offer a queue here in order to make flush threads have something to drain
   * <p> if queue already existed, do nothing
   * @param q queue to offer
   */
  boolean offer(SizeBoundedQueue q) {
    if (q.ready) {
      return false;
    }

    q.ready = true;
    boolean result = queue.offer(q);
    Thread top = waiters.poll();
    if (top != null) {
      LockSupport.unpark(top);
    }
    return result;
  }

  /**
   * Wait until at least one queue appears, then return it.
   * @param timeoutNanos if reaches this timeout and no queue appears, return null
   * @return the first queue offered
   */
  SizeBoundedQueue poll(long timeoutNanos) {
    final long deadline = System.nanoTime() + timeoutNanos;

    boolean wasInterrupted = false;
    boolean queued = false;
    Thread current = Thread.currentThread();

    while (queue.isEmpty()) {
      timeoutNanos = deadline - System.nanoTime();
      if (timeoutNanos <= 0L) {
        return null;
      }

      if (!queued) {
        waiters.offer(current);
        queued = true;
      }

      LockSupport.parkNanos(this, timeoutNanos);
      if (Thread.interrupted()) {
        wasInterrupted = true;
      }
    }

    if (wasInterrupted) {
      current.interrupt();
    }

    return queue.poll();
  }

  /**
   * Remove the key from the map so it can be offered again
   */
  void release(SizeBoundedQueue q) {
    q.ready = false;
  }

  /**
   * Clear everything
   */
  void clear() {
    waiters.clear();
    queue.clear();
  }
}
