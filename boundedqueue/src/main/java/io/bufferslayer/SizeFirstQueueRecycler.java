package io.bufferslayer;

import io.bufferslayer.Message.MessageKey;
import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by guohang.bao on 2017/5/4.
 */
final class SizeFirstQueueRecycler implements QueueRecycler {

  private static final Logger logger = LoggerFactory.getLogger(SizeFirstQueueRecycler.class);

  final Map<MessageKey, SizeBoundedQueue> keyToQueue;
  final Map<MessageKey, Long> keyToLastGet;

  final PriorityBlockingQueue<SizeBoundedQueue> bySize;
  final int pendingMaxMessages;
  final Strategy overflowStrategy;
  final long pendingKeepaliveNanos;

  private final Lock lock = new ReentrantLock();

  private static final Comparator<SizeBoundedQueue> SIZE_FIRST =
      new Comparator<SizeBoundedQueue>() {
        @Override
        public int compare(SizeBoundedQueue q1, SizeBoundedQueue q2) {
          return q2.count - q1.count;
        }
      };

  SizeFirstQueueRecycler(int pendingMaxMessages, Strategy overflowStrategy,
      long pendingKeepaliveNanos) {
    this.keyToQueue = new ConcurrentHashMap<>();
    this.keyToLastGet = new ConcurrentHashMap<>();

    this.bySize = new PriorityBlockingQueue<>(16, SIZE_FIRST);
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
          queue = new SizeBoundedQueue(pendingMaxMessages, overflowStrategy);
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
      return bySize.poll(timeout, unit);
    } catch (InterruptedException e) {
      logger.error("Interrupted leasing queue.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void recycle(SizeBoundedQueue queue) {
    if (queue != null) {
      bySize.offer(queue);
    }
  }

  static Long getOrDefault(Map<MessageKey, Long> map, MessageKey key, Long defaultValue) {
    Long v;
    return (((v = map.get(key)) != null) || map.containsKey(key))
        ? v
        : defaultValue;
  }

  @Override
  public LinkedList<SizeBoundedQueue> shrink() {
    LinkedList<SizeBoundedQueue> result = new LinkedList<>();
    Iterator<MessageKey> iterator = keyToLastGet.keySet().iterator();

    while (iterator.hasNext()) {
      MessageKey next = iterator.next();
      if (now() - getOrDefault(keyToLastGet, next, now()) > pendingKeepaliveNanos) {
        SizeBoundedQueue queue = keyToQueue.remove(next);
        keyToLastGet.remove(next);
        if (queue != null) {
          bySize.remove(queue);
          result.add(queue);
        }
      }
    }
    return result;
  }

  @Override
  public void clear() {
    keyToQueue.clear();
    keyToLastGet.clear();
    bySize.clear();
  }

  Collection<SizeBoundedQueue> elements() {
    return keyToQueue.values();
  }
}
