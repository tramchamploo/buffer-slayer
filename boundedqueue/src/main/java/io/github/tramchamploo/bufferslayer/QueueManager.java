package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/5/19.
 * Manages {@link SizeBoundedQueue}'s lifecycle
 */
class QueueManager {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  final Map<MessageKey, SizeBoundedQueue> keyToQueue;
  final Map<MessageKey, Long> keyToLastGet;

  final int pendingMaxMessages;
  final Strategy overflowStrategy;
  final long pendingKeepaliveNanos;

  private Callback createCallback;

  private final Lock lock = new ReentrantLock();

  QueueManager(int pendingMaxMessages,
               Strategy overflowStrategy,
               long pendingKeepaliveNanos) {
    this.keyToQueue = new HashMap<>();
    this.keyToLastGet = new HashMap<>();

    this.pendingMaxMessages = pendingMaxMessages;
    this.overflowStrategy = overflowStrategy;
    this.pendingKeepaliveNanos = pendingKeepaliveNanos;
  }

  interface Callback {
    void call(SizeBoundedQueue queue);
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
  SizeBoundedQueue get(MessageKey key) {
    lock.lock();
    try {
      return keyToQueue.get(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get a queue by message key if exists otherwise create one
   *
   * @param key message key related to the queue
   * @return the queue
   */
  SizeBoundedQueue getOrCreate(MessageKey key) {
    lock.lock();
    try {
      SizeBoundedQueue queue = keyToQueue.get(key);
      if (queue == null) {
        queue = new SizeBoundedQueue(pendingMaxMessages, overflowStrategy, key);
        keyToQueue.put(key, queue);
        onCreate(queue);
        logger.debug("Queue created, key: {}", key);
      }
      keyToLastGet.put(key, now());
      return queue;
    } finally {
      lock.unlock();
    }
  }

  private void onCreate(SizeBoundedQueue queue) {
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

    lock.lock();
    try {
      for (Iterator<MessageKey> iter = keyToLastGet.keySet().iterator(); iter.hasNext(); ) {
        MessageKey next = iter.next();
        if (now() - getOrDefault(keyToLastGet, next, now()) > pendingKeepaliveNanos) {
          SizeBoundedQueue q = keyToQueue.get(next);
          if (q == null || q.count > 0) continue;

          iter.remove();
          keyToQueue.remove(next);
          result.add(next);
        }
      }
    } finally {
      lock.unlock();
    }
    if (!result.isEmpty()) {
      logger.debug("Timeout queues removed, keys: {}", result);
    }
    return result;
  }

  static Long getOrDefault(Map<MessageKey, Long> map, MessageKey key, Long defaultValue) {
    Long v;
    return (((v = map.get(key)) != null) || map.containsKey(key))
        ? v
        : defaultValue;
  }

  /**
   * Clear everything
   */
  void clear() {
    lock.lock();
    try {
      keyToQueue.clear();
      keyToLastGet.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get all queues
   */
  Collection<SizeBoundedQueue> elements() {
    lock.lock();
    try {
      // return a copy here to avoid modification while traversing
      return new ArrayList<>(keyToQueue.values());
    } finally {
      lock.unlock();
    }
  }
}
