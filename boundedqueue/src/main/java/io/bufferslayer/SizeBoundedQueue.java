package io.bufferslayer;

import static io.bufferslayer.MessageDroppedException.dropped;
import static io.bufferslayer.OverflowStrategy.Strategy.*;

import com.google.common.annotations.VisibleForTesting;
import io.bufferslayer.Message.MessageKey;
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
  final Condition notEmpty = lock.newCondition();
  final Condition notFull = lock.newCondition();

  final int maxSize;
  final Strategy overflowStrategy;

  final Message[] elements;
  final MessageKey key;

  int count;
  int writePos;
  int readPos;

  SizeBoundedQueue(int maxSize, Strategy overflowStrategy, MessageKey key) {
    this.elements = new Message[maxSize];
    this.maxSize = maxSize;
    this.overflowStrategy = overflowStrategy;
    this.key = key;
  }

  // Used for testing
  SizeBoundedQueue(int maxSize, Strategy overflowStrategy) {
    this(maxSize, overflowStrategy, Message.STRICT_ORDER);
  }

  /**
   * Drop the last element
   */
  Message dropTail() {
    if (--writePos == -1) {
      writePos = elements.length - 1; // circle forward to the end of the array
    }
    Message tail = elements[writePos];
    elements[writePos] = null;
    count--;
    notFull.signal();
    return tail;
  }

  /**
   * Drop the first element
   */
  Message dropHead() {
    Message head = elements[readPos];
    elements[readPos] = null;
    if (++readPos == elements.length) {
      readPos = 0; // circle back to the front of the array
    }
    count--;
    notFull.signal();
    return head;
  }

  void enqueue(Message next) {
    try {
      while (isFull()) {
        notFull.await();
      }
      elements[writePos++] = next;

      if (writePos == elements.length) {
        writePos = 0; // circle back to the front of the array
      }

      count++;
      notEmpty.signal(); // alert any drainers
    } catch (InterruptedException e) {
    }
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
            DeferredHolder.reject(dropped(DropNew, next));
            return;
          case DropTail:
            Message tail = dropTail();
            enqueue(next);
            deferred.notify(1);
            DeferredHolder.reject(dropped(DropTail, tail));
            return;
          case DropHead:
            Message head = dropHead();
            enqueue(next);
            deferred.notify(1);
            DeferredHolder.reject(dropped(DropHead, head));
            return;
          case DropBuffer:
            List<Message> allElements = allElements();
            doClear();
            enqueue(next);
            deferred.notify(1);
            DeferredHolder.reject(dropped(DropBuffer, allElements));
            return;
          case Block:
            break;
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
          nanosLeft = notEmpty.awaitNanos(nanosLeft);
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
    for (int i = result; i > 0 && lock.hasWaiters(notFull); i--) {
      notFull.signal();
    }
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
    for (int i = drainedCount; i > 0 && lock.hasWaiters(notFull); i--) {
      notFull.signal();
    }
    return drainedCount;
  }
}
