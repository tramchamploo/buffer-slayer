package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropBuffer;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropNew;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropTail;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import io.github.tramchamploo.bufferslayer.internal.Promises;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
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
    boolean accept(MessagePromise<?> next);
  }

  private static final int DEFAULT_CAPACITY = 10;

  private final ReentrantLock lock = new ReentrantLock(false);
  private final Condition notFull = lock.newCondition();

  private final Strategy overflowStrategy;
  final int maxSize;
  final MessageKey key;

  private MessagePromise<?>[] elements;
  private int writePos;
  private int readPos;
  int count;

  // Set promise success if true, only used in benchmark
  private boolean _benchmark = false;

  private volatile long lastAccessNanos = System.nanoTime();
  // Flag indicates if this queue's size exceeds bufferedMaxMessages and ready to be drained
  volatile boolean ready = false;

  SizeBoundedQueue(int maxSize, Strategy overflowStrategy, MessageKey key) {
    int initialCapacity = DEFAULT_CAPACITY > maxSize ? maxSize : DEFAULT_CAPACITY;
    this.elements = new MessagePromise[initialCapacity];
    this.maxSize = maxSize;
    this.overflowStrategy = overflowStrategy;
    this.key = key;
  }

  // Used for testing
  SizeBoundedQueue(int maxSize, Strategy overflowStrategy) {
    this(maxSize, overflowStrategy, Message.SINGLE_KEY);
  }

  /**
   * Notify deferred if the element could be added or reject if it could not due to its
   * {@link OverflowStrategy.Strategy}.
   */
  void offer(MessagePromise<?> promise) {
    Message message = promise.message();

    lock.lock();
    try {
      ensureCapacity(count + 1);
      if (isFull()) {
        switch (overflowStrategy) {
          case DropNew:
            promise.setFailure(dropped(DropNew, message));
            return;
          case DropTail:
            MessagePromise<?> tail = dropTail();
            enqueue(promise);
            tail.setFailure(dropped(DropTail, tail.message()));
            return;
          case DropHead:
            MessagePromise<?> head = dropHead();
            enqueue(promise);
            head.setFailure(dropped(DropHead, head.message()));
            return;
          case DropBuffer:
            List<MessagePromise<?>> allElements = removeAll();
            doClear();
            enqueue(promise);
            Promises.allFail(allElements, DropBuffer);
            return;
          case Block:
            break;
          case Fail:
            throw new BufferOverflowException("Max size of " + count + " is reached.");
        }
      }
      enqueue(promise);
    } finally {
      lock.unlock();
    }
  }

  private void ensureCapacity(int minCapacity) {
    if (minCapacity > elements.length && minCapacity <= maxSize) {
      int oldCapacity = elements.length;
      int newCapacity = oldCapacity + (oldCapacity >> 1);
      if (newCapacity < minCapacity)
        newCapacity = minCapacity;
      if (newCapacity > maxSize)
        newCapacity = maxSize;
      // minCapacity is usually close to size, so this is a win:
      elements = Arrays.copyOf(elements, newCapacity);
      resetPositions(oldCapacity);
    }
  }

  private void resetPositions(int oldCapacity) {
    writePos = oldCapacity;

    int index = 0;
    while (elements[index] == null && index < elements.length) {
      index++;
    }
    readPos = index;
  }

  private boolean isFull() {
    return count == elements.length;
  }

  /**
   * Drop the last element
   */
  private MessagePromise<?> dropTail() {
    if (--writePos == -1) {
      writePos = elements.length - 1; // circle forward to the end of the array
    }
    MessagePromise<?> tail = elements[writePos];
    elements[writePos] = null;
    count--;
    notFull.signal();
    return tail;
  }

  /**
   * Drop the first element
   */
  private MessagePromise<?> dropHead() {
    MessagePromise<?> head = elements[readPos];
    elements[readPos] = null;
    if (++readPos == elements.length) {
      readPos = 0; // circle back to the front of the array
    }
    count--;
    notFull.signal();
    return head;
  }

  private void enqueue(MessagePromise<?> next) {
    try {
      while (isFull()) {
        notFull.await();
      }
      elements[writePos++] = next;

      if (writePos == elements.length) {
        writePos = 0; // circle back to the front of the array
      }

      count++;
      if (_benchmark) {
        next.setSuccess();
      }
    } catch (InterruptedException e) {
    }
  }

  /**
   * Set the benchmark mode
   */
  void _setBenchmarkMode(boolean benchmark) {
    _benchmark = benchmark;
  }

  private List<MessagePromise<?>> removeAll() {
    final List<MessagePromise<?>> result = new LinkedList<>();
    doDrain(new Consumer() {
      @Override
      public boolean accept(MessagePromise<?> next) {
        return result.add(next);
      }
    });
    return result;
  }

  int drainTo(Consumer consumer) {
    lock.lock();
    try {
      return doDrain(consumer);
    } finally {
      lock.unlock();
    }
  }

  private int doDrain(Consumer consumer) {
    int drainedCount = 0;

    while (drainedCount < count) {
      MessagePromise<?> next = elements[readPos];

      if (next == null) {
        break;
      }
      if (consumer.accept(next)) {
        drainedCount++;

        elements[readPos] = null;
        if (++readPos == elements.length) {
          readPos = 0; // circle back to the front of the array
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

  /**
   * Clears the queue unconditionally and returns count of elements cleared.
   */
  int clear() {
    lock.lock();
    try {
      return doClear();
    } finally {
      lock.unlock();
    }
  }

  private int doClear() {
    int result = count;

    count = readPos = writePos = 0;
    Arrays.fill(elements, null);
    for (int i = result; i > 0 && lock.hasWaiters(notFull); i--) {
      notFull.signal();
    }
    return result;
  }

  int size() {
    lock.lock();
    try {
      return count;
    } finally {
      lock.unlock();
    }
  }

  /**
   * set last access time to now
   */
  void recordAccess() {
    if (key != Message.SINGLE_KEY) {
      lastAccessNanos = System.nanoTime();
    }
  }

  /**
   * last time of this key accessed in nanoseconds
   */
  long lastAccessNanos() {
    return key == Message.SINGLE_KEY ? Long.MAX_VALUE : lastAccessNanos;
  }
}
