package io.bufferslayer;

import io.bufferslayer.Message.MessageKey;
import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/5/4.
 */
final class RoundRobinQueueRecycler implements QueueRecycler {

  private static final Logger logger = LoggerFactory.getLogger(RoundRobinQueueRecycler.class);

  final Map<MessageKey, SizeBoundedQueue> keyToQueue;
  final Map<MessageKey, Long> keyToLastGet;

  final LinkedBlockingQueue<SizeBoundedQueue> roundRobin;
  final int pendingMaxMessages;
  final Strategy overflowStrategy;
  final long pendingKeepaliveNanos;

  private final Lock lock = new ReentrantLock();

  RoundRobinQueueRecycler(int pendingMaxMessages, Strategy overflowStrategy,
      long pendingKeepaliveNanos) {
    this.keyToQueue = new ConcurrentHashMap<>();
    this.keyToLastGet = new ConcurrentHashMap<>();

    this.roundRobin = new LinkedBlockingQueue<>();
    this.pendingMaxMessages = pendingMaxMessages;
    this.overflowStrategy = overflowStrategy;
    this.pendingKeepaliveNanos = pendingKeepaliveNanos;
  }

  private static long now() {
    return System.nanoTime();
  }

  @Override
  public SizeBoundedQueue getOrCreate(MessageKey key) {
    SizeBoundedQueue queue = keyToQueue.get(key);
    if (queue == null) {
      // race condition initializing pending queue
      try {
        lock.lock();
        queue = keyToQueue.get(key);
        if (queue == null) {
          queue = new SizeBoundedQueue(pendingMaxMessages, overflowStrategy, key);
          keyToQueue.put(key, queue);
          recycle(queue);
        }
      } finally {
        lock.unlock();
      }
    }
    keyToLastGet.put(key, now());
    return queue;
  }

  @Override
  public SizeBoundedQueue lease(long timeout, TimeUnit unit) {
    try {
      return roundRobin.poll(timeout, unit);
    } catch (InterruptedException e) {
      logger.error("Interrupted leasing queue.", e);
    }
    return null;
  }

  @Override
  public void recycle(SizeBoundedQueue queue) {
    if (queue != null && // offer only when not dead
        (keyToQueue.containsValue(queue) || queue.count > 0)) {
      roundRobin.offer(queue);
    }
  }

  static Long getOrDefault(Map<MessageKey, Long> map, MessageKey key, Long defaultValue) {
    Long v;
    return (((v = map.get(key)) != null) || map.containsKey(key))
        ? v
        : defaultValue;
  }

  @Override
  public LinkedList<MessageKey> shrink() {
    LinkedList<MessageKey> result = new LinkedList<>();

    for (MessageKey next : keyToLastGet.keySet()) {
      if (now() - getOrDefault(keyToLastGet, next, now()) > pendingKeepaliveNanos) {
        try {
          lock.lock();
          SizeBoundedQueue q = keyToQueue.get(next);
          if (q == null || q.count > 0) continue;
          keyToLastGet.remove(next);
          keyToQueue.remove(next);
          roundRobin.remove(q);
          result.add(next);
        } finally {
          lock.unlock();
        }
      }
    }
    return result;
  }

  @Override
  public void clear() {
    keyToQueue.clear();
    keyToLastGet.clear();
    roundRobin.clear();
  }

  @Override
  public Collection<SizeBoundedQueue> elements() {
    return keyToQueue.values();
  }
}
