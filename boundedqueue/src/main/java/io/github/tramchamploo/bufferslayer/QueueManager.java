package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private final SizeBoundedQueueFactory queueFactory = SizeBoundedQueueFactory.factory();

  private Callback createCallback;

  QueueManager(int pendingMaxMessages,
               Strategy overflowStrategy,
               long pendingKeepaliveNanos) {
    this.keyToQueue = new ConcurrentHashMap<>();

    this.pendingMaxMessages = pendingMaxMessages;
    this.overflowStrategy = overflowStrategy;
    this.pendingKeepaliveNanos = pendingKeepaliveNanos;
  }

  interface Callback {
    void call(AbstractSizeBoundedQueue queue);
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
        onCreate(queue);
        if (logger.isDebugEnabled()) {
          logger.debug("Queue created, key: {}", key);
        }
      } else {
        queue = prev;
      }
    }
    queue.recordAccess();
    return queue;
  }

  private void onCreate(AbstractSizeBoundedQueue queue) {
    if (createCallback != null) createCallback.call(queue);
  }

  /**
   * set a callback for queue creation
   * @param callback callback to trigger after a queue is created
   */
  void onCreate(Callback callback) {
    createCallback = callback;
  }

  /**
   * Drop queues which exceed its keepalive
   */
  LinkedList<MessageKey> shrink() {
    LinkedList<MessageKey> result = new LinkedList<>();

      Iterator<Entry<MessageKey, AbstractSizeBoundedQueue>> iter;
      for (iter = keyToQueue.entrySet().iterator(); iter.hasNext(); ) {
        Entry<MessageKey, AbstractSizeBoundedQueue> entry = iter.next();
        MessageKey key = entry.getKey();
        AbstractSizeBoundedQueue q = entry.getValue();

        if (now() - q.lastAccessNanos() > pendingKeepaliveNanos) {
          if (!q.isEmpty()) continue;
          iter.remove();
          result.add(key);
        }
      }
    if (logger.isDebugEnabled() && !result.isEmpty()) {
      logger.debug("Timeout queues removed, keys: {}", result);
    }
    return result;
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
}
