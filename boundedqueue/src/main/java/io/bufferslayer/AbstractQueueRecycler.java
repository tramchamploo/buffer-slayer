package io.bufferslayer;

import io.bufferslayer.Message.MessageKey;
import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/5/19.
 */
abstract class AbstractQueueRecycler implements QueueRecycler {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  final Map<MessageKey, SizeBoundedQueue> keyToQueue;
  final Map<MessageKey, Long> keyToLastGet;

  final int pendingMaxMessages;
  final Strategy overflowStrategy;
  final long pendingKeepaliveNanos;

  private Callback createCallback;

  private final Lock lock = new ReentrantLock();

  AbstractQueueRecycler(int pendingMaxMessages,
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

  /**
   * recycler used to lease and recycle
   */
  abstract BlockingQueue<SizeBoundedQueue> recycler();

  private static long now() {
    return System.nanoTime();
  }

  @Override
  public SizeBoundedQueue getOrCreate(MessageKey key) {
    lock.lock();
    try {
      SizeBoundedQueue queue = keyToQueue.get(key);
      if (queue == null) {
        queue = new SizeBoundedQueue(pendingMaxMessages, overflowStrategy, key);
        keyToQueue.put(key, queue);
        recycle(queue);
        onCreate(queue);
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

  @Override
  public void createCallback(Callback callback) {
    createCallback = callback;
  }

  @Override
  public SizeBoundedQueue lease(long timeout, TimeUnit unit) {
    try {
      return recycler().poll(timeout, unit);
    } catch (InterruptedException e) {
      logger.error("Interrupted leasing queue.", e);
    }
    return null;
  }

  @Override
  public void recycle(SizeBoundedQueue queue) {
    if (queue != null) {
      lock.lock();
      try {
        // offer only when not dead
        if (keyToQueue.containsValue(queue) || queue.count > 0) {
          recycler().offer(queue);
        }
      } finally {
        lock.unlock();
      }
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

    lock.lock();
    try {
      for (Iterator<MessageKey> iter = keyToLastGet.keySet().iterator(); iter.hasNext(); ) {
        MessageKey next = iter.next();
        if (now() - getOrDefault(keyToLastGet, next, now()) > pendingKeepaliveNanos) {
          SizeBoundedQueue q = keyToQueue.get(next);
          if (q == null || q.count > 0) continue;

          iter.remove();
          keyToQueue.remove(next);
          recycler().remove(q);
          result.add(next);
        }
      }
    } finally {
      lock.unlock();
    }
    return result;
  }

  @Override
  public void clear() {
    lock.lock();
    try {
      keyToQueue.clear();
      keyToLastGet.clear();
      recycler().clear();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<SizeBoundedQueue> elements() {
    lock.lock();
    try {
      // return a copy here to avoid modification while traversing
      return new ArrayList<>(keyToQueue.values());
    } finally {
      lock.unlock();
    }
  }
}
