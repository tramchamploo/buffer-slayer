package io.bufferslayer;

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
class FlushSynchronizer<K> {

  private final Lock lock = new ReentrantLock();
  private final Condition available = lock.newCondition();

  private static final Object READY = new Object();
  private final Map<K, Object> keyToReady = new HashMap<>();

  boolean isReady() {
    return remaining() > 0;
  }

  void notifyOne(K key) {
    lock.lock();
    try {
      if (!keyToReady.containsKey(key)) {
        keyToReady.put(key, READY);
        available.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  void finish(K key) {
    if (keyToReady.containsKey(key)) {
      lock.lock();
      try {
        if (keyToReady.containsKey(key)) {
          keyToReady.remove(key);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  int remaining() {
    lock.lock();
    try {
      return keyToReady.size();
    } finally {
      lock.unlock();
    }
  }

  void await(long timeoutNanos) throws InterruptedException {
    lock.lock();
    try {
      long nanosLeft = timeoutNanos;
      while (!isReady()) {
        if (nanosLeft <= 0) break;
        nanosLeft = available.awaitNanos(nanosLeft);
      }
    } finally {
      lock.unlock();
    }
  }
}
