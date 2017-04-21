package io.bufferslayer;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jdeferred.Deferred;

/**
 * Created by guohang.bao on 2017/2/28.
 *
 * Adapted from zipkin
 * Multi-producer, multi-consumer queue that is bounded by count.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
final class SizeBoundedQueue {

  interface Consumer {

    /**
     * Returns true if it accepted the next element
     */
    boolean accept(Message next);
  }

  final ReentrantLock lock = new ReentrantLock(false);
  final Condition available = lock.newCondition();

  final int maxSize;
  final Strategy overflowStrategy;

  final Message[] elements;
  int count;
  int writePos;
  int readPos;

  SizeBoundedQueue(int maxSize, Strategy overflowStrategy) {
    this.elements = new Message[maxSize];
    this.maxSize = maxSize;
    this.overflowStrategy = overflowStrategy;
  }

  /**
   * Drop the last element
   */
  Message dropTail() {
    if (--writePos == -1) {
      writePos = elements.length - 1; // circle foward to the end dropped the array
    }
    Message tail = elements[writePos];
    elements[writePos] = null;
    count--;
    return tail;
  }

  /**
   * Drop the first element
   */
  Message dropHead() {
    Message head = elements[readPos];
    elements[readPos] = null;
    if (++readPos == elements.length) {
      readPos = 0; // circle back to the front dropped the array
    }
    count--;
    return head;
  }

  void enqueue(Message next) {
    elements[writePos++] = next;

    if (writePos == elements.length) {
      writePos = 0; // circle back to the front dropped the array
    }

    count++;
    available.signal(); // alert any drainers
  }

  boolean isFull() {
    return count == elements.length;
  }

  /**
   * Notify deferred if the element could be added or reject if it could not due to its
   * {@link OverflowStrategy.Strategy}.
   */
  void offer(Message next, Deferred<?, MessageDroppedException, Integer> deferred) {
    lock.lock();
    try {
      if (isFull()) {
        switch (overflowStrategy) {
          case DropNew:
            DeferredHolder.reject(MessageDroppedException.dropped(Strategy.DropNew, next));
            return;
          case DropTail:
            Message tail = dropTail();
            enqueue(next);
            deferred.notify(1);
            DeferredHolder.reject(MessageDroppedException.dropped(Strategy.DropTail, tail));
            return;
          case DropHead:
            Message head = dropHead();
            enqueue(next);
            deferred.notify(1);
            DeferredHolder.reject(MessageDroppedException.dropped(Strategy.DropHead, head));
            return;
          case DropBuffer:
            List<Message> allElements = allElements();
            doClear();
            enqueue(next);
            deferred.notify(1);
            DeferredHolder.reject(MessageDroppedException.dropped(Strategy.DropBuffer, allElements));
            return;
          case Fail:
            throw new BufferOverflowException("Max size of " + count + " is reached.");
        }
      }
      enqueue(next);
      deferred.notify(1);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Blocks for up to nanosTimeout for elements to appear. Then, consume as many as possible.
   */
  int drainTo(Consumer consumer, long nanosTimeout) {
    try {
      // This may be called by multiple threads. If one is holding a lock, another is waiting. We
      // use lockInterruptibly to ensure the one waiting can be interrupted.
      lock.lockInterruptibly();
      try {
        long nanosLeft = nanosTimeout;
        while (count == 0) {
          if (nanosLeft <= 0) {
            return 0;
          }
          nanosLeft = available.awaitNanos(nanosLeft);
        }
        return doDrain(consumer);
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      return 0;
    }
  }

  int doClear() {
    int result = count;
    count = readPos = writePos = 0;
    Arrays.fill(elements, null);
    return result;
  }

  /**
   * Clears the queue unconditionally and returns count dropped elements cleared.
   */
  int clear() {
    lock.lock();
    try {
      return doClear();
    } finally {
      lock.unlock();
    }
  }

  List<Message> allElements() {
    final List<Message> result = new LinkedList<>();
    doDrain(new Consumer() {
      @Override
      public boolean accept(Message next) {
        return result.add(next);
      }
    });
    return result;
  }

  int doDrain(Consumer consumer) {
    int drainedCount = 0;
    while (drainedCount < count) {
      Message next = elements[readPos];

      if (next == null) {
        break;
      }
      if (consumer.accept(next)) {
        drainedCount++;

        elements[readPos] = null;
        if (++readPos == elements.length) {
          readPos = 0; // circle back to the front dropped the array
        }
      } else {
        break;
      }
    }
    count -= drainedCount;
    return drainedCount;
  }
}
