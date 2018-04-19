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
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adapted from zipkin
 * Multi-producer, multi-consumer queue that is bounded by count.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
final class BlockingSizeBoundedQueue extends AbstractSizeBoundedQueue {
  private static final int DEFAULT_CAPACITY = 10;

  private final ReentrantLock lock = new ReentrantLock(false);
  private final Condition notFull = lock.newCondition();

  private final Strategy overflowStrategy;

  private MessagePromise<?>[] elements;
  private int writePos;
  private int readPos;
  private int count;

  BlockingSizeBoundedQueue(int maxSize, Strategy overflowStrategy, MessageKey key) {
    super(maxSize, key);
    int initialCapacity = DEFAULT_CAPACITY > maxSize ? maxSize : DEFAULT_CAPACITY;
    this.elements = new MessagePromise[initialCapacity];
    this.overflowStrategy = overflowStrategy;
  }

  // Used for testing
  BlockingSizeBoundedQueue(int maxSize, Strategy overflowStrategy) {
    this(maxSize, overflowStrategy, Message.SINGLE_KEY);
  }

  @Override
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
            List<MessagePromise<?>> allElements = copyAll();
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
    } catch (InterruptedException e) {
    }
  }

  private List<MessagePromise<?>> copyAll() {
    MessagePromise<?>[] copy = new MessagePromise[count];
    int length = elements.length - readPos;
    System.arraycopy(elements, readPos, copy, 0, length);
    System.arraycopy(elements, 0, copy, length, writePos);
    return Arrays.asList(copy);
  }

  @Override
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

        if (_benchmark) {
          next.setSuccess();
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

  @Override
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

  @Override
  int size() {
    lock.lock();
    try {
      return count;
    } finally {
      lock.unlock();
    }
  }

  @Override
  boolean isEmpty() {
    lock.lock();
    try {
      return count == 0;
    } finally {
      lock.unlock();
    }
  }
}
