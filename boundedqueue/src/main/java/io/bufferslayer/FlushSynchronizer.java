package io.bufferslayer;

import java.util.concurrent.atomic.AtomicInteger;
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
  private final AtomicInteger ready = new AtomicInteger();

  boolean isReady() {
    return remaining() > 0;
  }

  void notifyOne() {
    lock.lock();
    try {
      ready.incrementAndGet();
      available.signal();
    } finally {
      lock.unlock();
    }
  }

  int finish() {
    int last = ready.get();
    while (last > 0) {
      int update = last - 1;
      if (ready.compareAndSet(last, update)) return update;
      last = ready.get();
    }
    return last;
  }

  int remaining() {
    return ready.get();
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
