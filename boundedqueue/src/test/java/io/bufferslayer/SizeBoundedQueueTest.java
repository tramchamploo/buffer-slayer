package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.jdeferred.Deferred;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by guohang.bao on 2017/3/14.
 */
public class SizeBoundedQueueTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  OverflowStrategy.Strategy dropNew = OverflowStrategy.dropNew;
  SizeBoundedQueue queue = new SizeBoundedQueue(10, dropNew);

  private Deferred<Object, MessageDroppedException, Integer> newDeferred(Message next) {
    return DeferredHolder.newDeferred(next.id);
  }

  @Test
  public void offer_failsWhenFull_size() {
    AtomicBoolean success = new AtomicBoolean(true);
    for (int i = 0; i < queue.maxSize; i++) {
      Message next = newMessage(i);
      queue.offer(next, newDeferred(next));
      assertTrue(success.get());
    }

    Message shouldFail = newMessage(0);
    Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(
        shouldFail);
    deferred.promise().fail(obj -> success.set(false));
    queue.offer(shouldFail, deferred);
    assertFalse(success.get());
  }

  @Test
  public void offer_updatesCount() {
    for (int i = 0; i < queue.maxSize; i++) {
      Message next = newMessage(i);
      queue.offer(next, newDeferred(next));
    }
    assertEquals(10, queue.count);
  }

  @Test
  public void expectExceptionWhenFullSize_failStrategy() {
    SizeBoundedQueue queue = new SizeBoundedQueue(10, OverflowStrategy.fail);
    for (int i = 0; i < queue.maxSize; i++) {
      Message next = newMessage(i);
      queue.offer(next, newDeferred(next));
    }

    thrown.expect(BufferOverflowException.class);
    thrown.expectMessage("Max size of 10 is reached.");
    Message overflow = newMessage(10);
    queue.offer(overflow, newDeferred(overflow));
  }

  @Test
  public void dropHeadWhenFull_dropHeadStrategy() throws InterruptedException {
    SizeBoundedQueue queue = new SizeBoundedQueue(10, OverflowStrategy.dropHead);
    CountDownLatch countDown = new CountDownLatch(1);

    for (int i = 0; i < queue.maxSize; i++) {
      Message next = newMessage(i);
      Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(next);
      queue.offer(next, deferred);
      if (i == 0) {
        deferred.fail(ex -> {
          assertEquals(0, ((TestMessage)ex.dropped.get(0)).key);
          countDown.countDown();
        });
      }
    }
    Message overflow = newMessage(queue.maxSize);
    queue.offer(overflow, newDeferred(overflow));
    countDown.await();

    Object[] ids = collectKeys(queue);
    assertArrayEquals(new Object[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, ids);
  }

  @Test
  public void dropTailWhenFull_dropTailStrategy() throws InterruptedException {
    SizeBoundedQueue queue = new SizeBoundedQueue(10, OverflowStrategy.dropTail);
    CountDownLatch countDown = new CountDownLatch(1);

    for (int i = 0; i < queue.maxSize; i++) {
      Message next = newMessage(i);
      Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(next);
      queue.offer(next, deferred);
      if (i == queue.maxSize - 1) {
        deferred.fail(ex -> {
          assertEquals(queue.maxSize - 1, ((TestMessage)ex.dropped.get(0)).key);
          countDown.countDown();
        });
      }
    }
    Message overflow = newMessage(queue.maxSize);
    Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(overflow);
    queue.offer(overflow, deferred);
    countDown.await();

    Object[] ids = collectKeys(queue);
    assertArrayEquals(new Object[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 10}, ids);
  }

  @Test
  public void dropBufferWhenFull_dropBufferStrategy() throws InterruptedException {
    SizeBoundedQueue queue = new SizeBoundedQueue(10, OverflowStrategy.dropBuffer);
    CountDownLatch countDown = new CountDownLatch(1);

    for (int i = 0; i < queue.maxSize; i++) {
      TestMessage next = newMessage(i);
      Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(next);
      queue.offer(next, deferred);

      if (i == 0) {
        deferred.fail(ex -> {
          assertEquals(10, ex.dropped.size());
          assertArrayEquals(new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
              ex.dropped.stream().map(d -> ((TestMessage)d).key).toArray());
          countDown.countDown();
        });
      }
    }
    Message overflow = newMessage(queue.maxSize);
    queue.offer(overflow, newDeferred(overflow));
    countDown.await();

    Object[] ids = collectKeys(queue);
    assertArrayEquals(new Object[]{10}, ids);
  }

  @Test
  public void blockCallerWhenFull_blockStrategy() throws InterruptedException {
    SizeBoundedQueue queue = new SizeBoundedQueue(10, OverflowStrategy.block);
    for (int i = 0; i < queue.maxSize; i++) {
      TestMessage next = newMessage(i);
      queue.offer(next, newDeferred(next));
    }

    CountDownLatch countDown = new CountDownLatch(1);
    new Thread(() -> {
      TestMessage shouldBlock = newMessage(10);
      queue.offer(shouldBlock, newDeferred(shouldBlock));
      countDown.countDown();
    }).start();
    assertFalse(countDown.await(1, TimeUnit.MILLISECONDS));
    queue.drainTo(next -> true, 0);
    assertTrue(countDown.await(1, TimeUnit.MILLISECONDS));
  }

  @Test
  public void circular() {
    SizeBoundedQueue queue = new SizeBoundedQueue(10, dropNew);

    List<Message> polled = new ArrayList<>();
    SizeBoundedQueue.Consumer consumer = polled::add;

    // Offer more than the capacity, flushing via poll on interval
    for (byte i = 0; i < 15; i++) {
      Message next = newMessage(i);
      queue.offer(next, newDeferred(next));
      queue.drainTo(consumer, 1);
    }

    // ensure we have all dropped the elements
    Object[] ids = polled.stream()
        .map(m -> ((TestMessage) m).key)
        .collect(Collectors.toList())
        .toArray();

    assertArrayEquals(new Object[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, ids);
  }

  private Object[] collectKeys(SizeBoundedQueue queue) {
    List<Message> polled = new ArrayList<>();
    SizeBoundedQueue.Consumer consumer = polled::add;
    queue.drainTo(consumer, 1);
    return polled.stream()
        .map(m -> ((TestMessage) m).key)
        .collect(Collectors.toList())
        .toArray();
  }
}
