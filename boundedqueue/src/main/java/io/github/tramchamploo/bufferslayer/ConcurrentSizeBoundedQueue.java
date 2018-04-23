package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.Block;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropBuffer;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropNew;
import static io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy.DropTail;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import io.github.tramchamploo.bufferslayer.internal.Promises;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This use {@link java.util.concurrent.ConcurrentLinkedDeque} to implement a lock-free queue
 */
final class ConcurrentSizeBoundedQueue extends AbstractSizeBoundedQueue {

  // ConcurrentLinkedDeque.size() is an expensive operation.
  // This size has a weak consistency on the real queue's
  private final AtomicInteger size = new AtomicInteger();

  private final Strategy overflowStrategy;

  private final ConcurrentLinkedDeque<MessagePromise<?>> deque = new ConcurrentLinkedDeque<>();
  // Lock used to block producers, only used when Block strategy is specified
  private final ReentrantLock blocker;
  private final Condition notFull;

  ConcurrentSizeBoundedQueue(int maxSize, Strategy overflowStrategy, MessageKey key) {
    super(maxSize, key);
    this.overflowStrategy = overflowStrategy;
    if (overflowStrategy == Block) {
      blocker = new ReentrantLock();
      notFull = blocker.newCondition();
    } else {
      blocker = null;
      notFull = null;
    }
  }

  // Used for testing
  ConcurrentSizeBoundedQueue(int maxSize, Strategy overflowStrategy) {
    this(maxSize, overflowStrategy, Message.SINGLE_KEY);
  }

  @Override
  void offer(MessagePromise<?> promise) {
    for (;;) {
      int now = size.get();

      if (now >= maxSize) {
        switch (overflowStrategy) {
          case DropNew:
            // Double check if the size has been changed
            if ((now = size.get()) >= maxSize) {
              promise.setFailure(dropped(DropNew, promise.message()));
              return;
            }
            // Continue to offer
            break;

          case DropTail:
            if (size.compareAndSet(now, now - 1)) {
              MessagePromise<?> tail = deque.pollLast();
              // tail can be null if queue gets drained
              if (tail != null)
                tail.setFailure(dropped(DropTail, tail.message()));
              else
                // Reset counter
                size.incrementAndGet();
            }
            // Lost CAS race to another thread
            // Or successfully dropped tail
            // Or tail get drained; re-read counter
            continue;

          case DropHead:
            if (size.compareAndSet(now, now - 1)) {
              MessagePromise<?> head = deque.pollFirst();
              // head can be null if queue gets drained
              if (head != null)
                head.setFailure(dropped(DropHead, head.message()));
              else
                // Reset counter
                size.incrementAndGet();
            }
            // Lost CAS race to another thread
            // Or successfully dropped head
            // Or head get drained; re-read counter
            continue;

          case DropBuffer:
            List<MessagePromise<?>> allElements = removeAll(deque);
            // Nodes already removed, counter definitely should follow
            int dropped;
            if ((dropped = allElements.size()) > 0)
              incSize(now, -dropped);

            Promises.allFail(allElements, DropBuffer);
            continue;

          case Block:
            handleBlock();
            continue;

          case Fail:
            // Double check if the size has been changed
            if ((now = size.get()) >= maxSize)
              throw new BufferOverflowException("Max size of " + maxSize + " is reached.");
            break;
        }
      }

      if (size.compareAndSet(now, now + 1)) {
        deque.offer(promise);
        return;
      }
      // Lost CAS race to another thread; re-read counter
    }
  }

  private void incSize(int expected, int delta) {
    int prev = expected;
    while (!size.compareAndSet(prev, prev + delta)) {
      prev = size.get();
    }
  }

  private void handleBlock() {
    boolean wasInterrupted = false;

    blocker.lock();
    try {
      while (size.get() >= maxSize) {
        try {
          notFull.await();
        } catch (InterruptedException e) {
          wasInterrupted = true;
        }
      }
    } finally {
      blocker.unlock();
    }

    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static List<MessagePromise<?>> removeAll(ConcurrentLinkedDeque<MessagePromise<?>> deque) {
    final List<MessagePromise<?>> result = new LinkedList<>();
    MessagePromise<?> promise;

    while ((promise = deque.pollFirst()) != null) {
      result.add(promise);
    }
    return result;
  }

  @Override
  int drainTo(Consumer consumer) {
    MessagePromise<?> promise;
    int drained = 0;

    while ((promise = deque.poll()) != null) {
      if (!consumer.accept(promise)) {
        // deque may have been changed since last poll, but never mind
        deque.offerFirst(promise);
        break;
      }
      drained++;
      if (_benchmark) {
        promise.setSuccess();
      }
    }
    size.addAndGet(-drained);

    // Unpark waiting threads if block strategy is applied
    if (notFull != null && drained > 0) {
      blocker.lock();
      try {
        for (int count = drained; count > 0 && blocker.hasWaiters(notFull); count--) {
          notFull.signal();
        }
      } finally {
        blocker.unlock();
      }
    }
    return drained;
  }

  @Override
  int clear() {
    return drainTo(new Consumer() {
      @Override
      public boolean accept(MessagePromise<?> next) {
        return true;
      }
    });
  }

  @Override
  int size() {
    return size.get();
  }

  @Override
  boolean isEmpty() {
    return deque.isEmpty();
  }
}
