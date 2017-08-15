package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropBuffer;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropNew;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropTail;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jdeferred.Deferred;

/**
 * Adapted from zipkin
 * Multi-producer, multi-consumer queue that is bounded by count.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
@SuppressWarnings("unchecked")
final class SizeBoundedQueue {

  interface Consumer<M extends Message> {

    /**
     * Returns true if it accepted the next element
     */
    boolean accept(SendingTask<M> next);
  }

  static final int DEFAULT_CAPACITY = 10;

  final ReentrantLock lock = new ReentrantLock(false);
  final Condition notFull = lock.newCondition();

  final int maxSize;
  final Strategy overflowStrategy;
  final MessageKey key;

  SendingTask[] elements;
  int count;

  final MessageCounter messageCounter;

  int writePos;
  int readPos;

  SizeBoundedQueue(int maxSize, Strategy overflowStrategy, MessageKey key, MessageCounter messageCounter) {
    int initialCapacity = DEFAULT_CAPACITY > maxSize ? maxSize : DEFAULT_CAPACITY;
    this.elements = new SendingTask[initialCapacity];
    this.maxSize = maxSize;
    this.overflowStrategy = overflowStrategy;
    this.key = key;
    this.messageCounter = messageCounter;
  }

  // Used for testing
  SizeBoundedQueue(int maxSize, Strategy overflowStrategy) {
    this(maxSize, overflowStrategy, Message.SINGLE_KEY, MessageCounter.maxOf(50));
  }

  /**
   * Notify deferred if the element could be added or reject if it could not due to its
   * {@link OverflowStrategy.Strategy}.
   */
  void offer(Message next, Deferred<?, MessageDroppedException, Integer> deferred) {
    SendingTask task = new SendingTask(next, deferred);
    lock.lock();
    try {
      ensureCapacity(count + 1);
      if (isFull() || messageCounter.isMaximum()) {
        switch (overflowStrategy) {
          case DropNew:
            deferred.reject(dropped(DropNew, next));
            return;
          case DropTail:
            SendingTask tail = dropTail();
            enqueue(task);
            tail.deferred.reject(dropped(DropTail, tail.message));
            return;
          case DropHead:
            SendingTask head = dropHead();
            enqueue(task);
            head.deferred.reject(dropped(DropHead, head.message));
            return;
          case DropBuffer:
            List<SendingTask> allElements = removeAll();
            doClear();
            enqueue(task);
            rejectAll(allElements, DropBuffer);
            return;
          case Block:
            break;
          case Fail:
            throw new BufferOverflowException("Max size of " + count + " is reached.");
        }
      }
      enqueue(task);
    } finally {
      lock.unlock();
    }
  }

  private void rejectAll(List<SendingTask> tasks, Strategy overflowStrategy) {
    Object[] messageAndDeferred = SendingTask.unzip(tasks);
    List<Message> messages = (List<Message>) messageAndDeferred[0];
    List<Deferred> deferreds = (List<Deferred>) messageAndDeferred[1];
    for (Deferred deferred : deferreds) {
      deferred.reject(dropped(overflowStrategy, messages));
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
  private SendingTask dropTail() {
    if (--writePos == -1) {
      writePos = elements.length - 1; // circle forward to the end of the array
    }
    SendingTask tail = elements[writePos];
    elements[writePos] = null;
    count--;
    notFull.signal();
    return tail;
  }

  /**
   * Drop the first element
   */
  private SendingTask dropHead() {
    SendingTask head = elements[readPos];
    elements[readPos] = null;
    if (++readPos == elements.length) {
      readPos = 0; // circle back to the front of the array
    }
    count--;
    notFull.signal();
    return head;
  }

  private void enqueue(SendingTask next) {
    try {
      while (isFull() || messageCounter.isMaximum()) {
        notFull.await();
      }
      elements[writePos++] = next;

      if (writePos == elements.length) {
        writePos = 0; // circle back to the front of the array
      }

      count++;
      messageCounter.increment();
      next.deferred.notify(1); // notify for benchmark
    } catch (InterruptedException e) {
    }
  }

  private List<SendingTask> removeAll() {
    final List<SendingTask> result = new LinkedList<>();
    doDrain(new Consumer() {
      @Override
      public boolean accept(SendingTask next) {
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

  @SuppressWarnings("unchecked")
  private int doDrain(Consumer consumer) {
    int drainedCount = 0;

    while (drainedCount < count) {
      SendingTask next = elements[readPos];

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
    messageCounter.add(-drainedCount);

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
    messageCounter.add(-result);
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
