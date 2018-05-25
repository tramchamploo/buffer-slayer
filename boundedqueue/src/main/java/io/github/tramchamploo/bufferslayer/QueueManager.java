package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages {@link AbstractSizeBoundedQueue}'s lifecycle
 */
class QueueManager {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  final ConcurrentMap<MessageKey, AbstractSizeBoundedQueue> keyToQueue;

  private final int pendingMaxMessages;
  private final Strategy overflowStrategy;
  private final long pendingKeepaliveNanos;

  private final TimeDriven<MessageKey> timeDriven;
  private final ReporterMetrics metrics;
  private final HashedWheelTimer timer;

  private final SizeBoundedQueueFactory queueFactory = SizeBoundedQueueFactory.factory();
  private CreateCallback createCallback;

  QueueManager(int pendingMaxMessages, Strategy overflowStrategy,
      long pendingKeepaliveNanos, TimeDriven<MessageKey> timeDriven,
      ReporterMetrics metrics, HashedWheelTimer timer) {
    this.keyToQueue = new ConcurrentHashMap<>();

    this.pendingMaxMessages = pendingMaxMessages;
    this.overflowStrategy = overflowStrategy;
    this.pendingKeepaliveNanos = pendingKeepaliveNanos;

    this.timeDriven = timeDriven;
    this.metrics = metrics;
    this.timer = timer;
  }

  private static long now() {
    return System.nanoTime();
  }

  /**
   * Get a queue by message key
   *
   * @param key message key related to the queue
   * @return the queue if existed otherwise null
   */
  AbstractSizeBoundedQueue get(MessageKey key) {
    return keyToQueue.get(key);
  }

  /**
   * Get a queue by message key if exists otherwise create one
   *
   * @param key message key related to the queue
   * @return the queue
   */
  AbstractSizeBoundedQueue getOrCreate(MessageKey key) {
    AbstractSizeBoundedQueue queue = keyToQueue.get(key);
    if (queue == null) {
      queue = queueFactory.newQueue(pendingMaxMessages, overflowStrategy, key);
      AbstractSizeBoundedQueue prev = keyToQueue.putIfAbsent(key, queue);

      if (prev == null) {
        // schedule a clean task
        timer.newTimeout(new CleanTask(queue),
            pendingKeepaliveNanos, TimeUnit.NANOSECONDS);
        // call listener
        onCreate(queue);
        if (logger.isDebugEnabled()) {
          logger.debug("Queue created, key: {}", key);
        }
      } else {
        queue = prev;
        // race failed doesn't record access because too near
      }
    } else {
      queue.recordAccess();
    }
    return queue;
  }

  private void onCreate(AbstractSizeBoundedQueue queue) {
    if (createCallback != null) createCallback.call(queue);
  }

  /**
   * set a callback for queue creation
   * @param callback callback to trigger after a queue is created
   */
  void onCreate(CreateCallback callback) {
    createCallback = callback;
  }

  /**
   * Clear everything
   */
  void clear() {
    keyToQueue.clear();
  }

  /**
   * Get all queues
   */
  Collection<AbstractSizeBoundedQueue> elements() {
    return new ArrayList<>(keyToQueue.values());
  }

  interface CreateCallback {
    void call(AbstractSizeBoundedQueue queue);
  }

  /**
   * Remove queue if exceed timeout else reschedule
   */
  private final class CleanTask implements TimerTask {
    private final AbstractSizeBoundedQueue queue;

    CleanTask(AbstractSizeBoundedQueue queue) {
      this.queue = queue;
    }

    @Override
    public void run(Timeout timeout) {
      long now = now();
      long deadline;
      if (queue.isEmpty() &&
          (deadline = queue.lastAccessNanos() + pendingKeepaliveNanos) < now &&
          // Workaround for long overflow
          deadline > 0) {
        // Timeout, do clean job
        MessageKey key = queue.key;

        metrics.removeFromQueuedMessages(key);
        if (timeDriven.isTimerActive(key)) {
          timeDriven.cancelTimer(key);
        }
        keyToQueue.remove(key);

        if (logger.isDebugEnabled()) {
          logger.debug("Timeout queue removed, key: {}", key);
        }
      } else {
        long delay = Math.max(0L, pendingKeepaliveNanos - (now - queue.lastAccessNanos()));
        timeout.timer().newTimeout(this, delay, TimeUnit.NANOSECONDS);
      }
    }
  }
}
