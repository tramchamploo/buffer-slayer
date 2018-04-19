package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.github.tramchamploo.bufferslayer.internal.Future;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public abstract class AbstractSizeBoundedQueueTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  OverflowStrategy.Strategy dropNew = OverflowStrategy.dropNew;
  AbstractSizeBoundedQueue queue = newQueue(dropNew);

  protected abstract AbstractSizeBoundedQueue newQueue(OverflowStrategy.Strategy strategy);

  @Test
  public void offer_failsWhenFull_size() {
    AtomicBoolean success = new AtomicBoolean(true);
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(newPromise(i));
      assertTrue(success.get());
    }

    MessagePromise<Integer> shouldFail = newPromise(0);
    shouldFail.addListener(future -> success.set(false));
    queue.offer(shouldFail);
    assertFalse(success.get());
  }

  @Test
  public void offer_updatesCount() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(newPromise(i));
    }
    assertEquals(queue.maxSize, queue.size());
  }

  @Test
  public void expectExceptionWhenFullSize_failStrategy() {
    AbstractSizeBoundedQueue queue = newQueue(OverflowStrategy.fail);
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(newPromise(i));
    }

    thrown.expect(BufferOverflowException.class);
    thrown.expectMessage("Max size of 16 is reached.");
    MessagePromise<Integer> overflow = newPromise(11);
    queue.offer(overflow);
  }

  @Test
  public void dropHeadWhenFull_dropHeadStrategy() throws InterruptedException {
    AbstractSizeBoundedQueue queue = newQueue(OverflowStrategy.dropHead);
    CountDownLatch countDown = new CountDownLatch(1);

    for (int i = 0; i < queue.maxSize; i++) {
      MessagePromise<Integer> next = newPromise(i);
      queue.offer(next);
      if (i == 0) {
        next.addListener(future -> {
          assertEquals(0, getKey(future));
          countDown.countDown();
        });
      }
    }
    MessagePromise<Integer> overflow = newPromise(queue.maxSize);
    queue.offer(overflow);
    countDown.await();

    Object[] ids = collectKeys(queue);
    assertArrayEquals(new Object[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, ids);
  }

  @Test
  public void dropTailWhenFull_dropTailStrategy() throws InterruptedException {
    AbstractSizeBoundedQueue queue = newQueue(OverflowStrategy.dropTail);
    CountDownLatch countDown = new CountDownLatch(1);

    for (int i = 0; i < queue.maxSize; i++) {
      MessagePromise<Integer> next = newPromise(i);
      queue.offer(next);
      if (i == queue.maxSize - 1) {
        next.addListener(future -> {
          assertEquals(queue.maxSize - 1, getKey(future));
          countDown.countDown();
        });
      }
    }
    MessagePromise<Integer> overflow = newPromise(queue.maxSize);
    queue.offer(overflow);
    countDown.await();

    Object[] ids = collectKeys(queue);
    assertArrayEquals(new Object[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}, ids);
  }

  @Test
  public void dropBufferWhenFull_dropBufferStrategy() throws InterruptedException {
    AbstractSizeBoundedQueue queue = newQueue(OverflowStrategy.dropBuffer);
    CountDownLatch countDown = new CountDownLatch(1);

    for (int i = 0; i < queue.maxSize; i++) {
      MessagePromise<Integer> next = newPromise(i);
      queue.offer(next);
      if (i == 0) {
        next.addListener(future -> {
          MessageDroppedException cause = (MessageDroppedException) future.cause();
          assertEquals(16, cause.dropped.size());
          assertArrayEquals(new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
              cause.dropped.stream().map(d -> ((TestMessage) d).key).toArray());
          countDown.countDown();
        });
      }
    }

    MessagePromise<Integer> overflow = newPromise(queue.maxSize);
    queue.offer(overflow);
    countDown.await();

    Object[] ids = collectKeys(queue);
    assertArrayEquals(new Object[]{16}, ids);
  }

  @Test
  public void blockCallerWhenFull_blockStrategy() throws InterruptedException {
    AbstractSizeBoundedQueue queue = newQueue(OverflowStrategy.block);
    for (int i = 0; i < queue.maxSize; i++) {
      MessagePromise<Integer> next = newPromise(i);
      queue.offer(next);
    }

    CountDownLatch countDown = new CountDownLatch(1);
    new Thread(() -> {
      MessagePromise<Integer> shouldBlock = newPromise(10);
      queue.offer(shouldBlock);
      countDown.countDown();
    }).start();
    assertFalse(countDown.await(10, TimeUnit.MILLISECONDS));
    queue.drainTo(next -> true);
    assertTrue(countDown.await(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void circular() {
    AbstractSizeBoundedQueue queue = newQueue(dropNew);

    List<MessagePromise> polled = new ArrayList<>();
    BlockingSizeBoundedQueue.Consumer consumer = polled::add;

    // Offer more than capacity, flushing via poll on interval
    for (byte i = 0; i < 20; i++) {
      MessagePromise<Integer> next = newPromise(i);
      queue.offer(next);
      queue.drainTo(consumer);
    }

    // ensure we have all of the elements
    Object[] ids = polled.stream()
        .map(m -> ((TestMessage) m.message()).key)
        .toArray();

    assertArrayEquals(
        new Object[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, ids);
  }

  private int getKey(Future<? super Integer> future) {
    return ((TestMessage) (((MessageDroppedException) future.cause()).dropped.get(0))).key;
  }

  private Object[] collectKeys(AbstractSizeBoundedQueue queue) {
    List<MessagePromise> polled = new ArrayList<>();
    AbstractSizeBoundedQueue.Consumer consumer = polled::add;
    queue.drainTo(consumer);
    return polled.stream()
        .map(m -> ((TestMessage) m.message()).key)
        .toArray();
  }

  private static MessagePromise<Integer> newPromise(int key) {
    return newMessage(key).newPromise();
  }
}
