package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.DeferredHolder.reject;
import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropBuffer;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropNew;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropTail;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jdeferred.Deferred;

/**
 * Created by tramchamploo on 2017/2/28.
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

  static final int DEFAULT_CAPACITY = 10;

  final ReentrantLock lock = new ReentrantLock(true);
  final Condition notEmpty = lock.newCondition();
  final Condition notFull = lock.newCondition();

  final int maxSize;
  final Strategy overflowStrategy;
  final MessageKey key;

  Message[] elements;
  int count;
  int writePos;
  int readPos;

  SizeBoundedQueue(int maxSize, Strategy overflowStrategy, MessageKey key) {
    int initialCapacity = DEFAULT_CAPACITY > maxSize ? maxSize : DEFAULT_CAPACITY;
    this.elements = new Message[initialCapacity];
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
  void offer(Message next, Deferred<?, MessageDroppedException, Integer> deferred) {
    lock.lock();
    try {
      ensureCapacity(count + 1);
      if (isFull()) {
        switch (overflowStrategy) {
          case DropNew:
            reject(dropped(DropNew, next));
            return;
          case DropTail:
            Message tail = dropTail();
            enqueue(next);
            deferred.notify(1);
            reject(dropped(DropTail, tail));
            return;
          case DropHead:
            Message head = dropHead();
            enqueue(next);
            deferred.notify(1);
            reject(dropped(DropHead, head));
            return;
          case DropBuffer:
            List<Message> allElements = removeAll();
            doClear();
            enqueue(next);
            deferred.notify(1);
            reject(dropped(DropBuffer, allElements));
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
  private Message dropTail() {
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
  private Message dropHead() {
    Message head = elements[readPos];
    elements[readPos] = null;
    if (++readPos == elements.length) {
      readPos = 0; // circle back to the front of the array
    }
    count--;
    notFull.signal();
    return head;
  }

  private void enqueue(Message next) {
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

  private List<Message> removeAll() {
    final List<Message> result = new LinkedList<>();
    doDrain(new Consumer() {
      @Override
      public boolean accept(Message next) {
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
      Message next = elements[readPos];

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
}
