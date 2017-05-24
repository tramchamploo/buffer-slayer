package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import io.bufferslayer.AbstractQueueRecycler.Callback;
import io.bufferslayer.Message.MessageKey;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jdeferred.impl.DeferredObject;
import org.junit.Test;

/**
 * Created by tramchamploo on 2017/5/19.
 */
@SuppressWarnings("unchecked")
public abstract class QueueRecyclerTest {

  abstract AbstractQueueRecycler newRecycler();

  static DeferredObject newDeferred() {
    return new DeferredObject();
  }

  @Test
  public void getAfterCreate() {
    QueueRecycler recycler = newRecycler();
    SizeBoundedQueue q = recycler.getOrCreate(Message.STRICT_ORDER);
    assertEquals(q, recycler.getOrCreate(Message.STRICT_ORDER));
  }

  @Test
  public void callbackTriggeredAfterCreated() throws InterruptedException {
    QueueRecycler recycler = newRecycler();
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();
    recycler.createCallback(new Callback() {
      @Override
      public void call(SizeBoundedQueue queue) {
        assertEquals(Message.STRICT_ORDER, queue.key);
        callCount.incrementAndGet();
        countDown.countDown();
      }
    });
    recycler.getOrCreate(Message.STRICT_ORDER);
    countDown.await();
    recycler.getOrCreate(Message.STRICT_ORDER);
    // only triggered once created
    assertEquals(1, callCount.get());
  }

  @Test
  public void keepAlive() throws InterruptedException {
    AbstractQueueRecycler recycler = newRecycler();
    SizeBoundedQueue q = recycler.getOrCreate(Message.STRICT_ORDER);
    TimeUnit.MILLISECONDS.sleep(10);

    assertEquals(Message.STRICT_ORDER, recycler.shrink().get(0));
    assertEquals(0, recycler.recycler().size());
    assertEquals(0, recycler.keyToQueue.size());
    assertEquals(0, recycler.keyToLastGet.size());
    SizeBoundedQueue q1 = recycler.getOrCreate(Message.STRICT_ORDER);
    assertNotEquals(q, q1);

    TimeUnit.MILLISECONDS.sleep(10);
    recycler.getOrCreate(Message.STRICT_ORDER);
    assertEquals(1, recycler.recycler().size());
    assertEquals(1, recycler.keyToQueue.size());
    assertEquals(1, recycler.keyToLastGet.size());
    assertEquals(q1, recycler.getOrCreate(Message.STRICT_ORDER));
  }

  @Test
  public void notLeaseShouldDieQueue() throws InterruptedException {
    QueueRecycler recycler = newRecycler();
    MessageKey key = newMessage(0).asMessageKey();
    recycler.getOrCreate(key);
    SizeBoundedQueue q = recycler.lease(1, TimeUnit.MILLISECONDS);
    TimeUnit.MILLISECONDS.sleep(10);
    assertEquals(key, recycler.shrink().get(0));
    recycler.recycle(q);
    assertNull(recycler.lease(1, TimeUnit.MILLISECONDS));
  }

  @Test
  public void leaseTimeout() throws InterruptedException {
    QueueRecycler recycler = newRecycler();
    for (int i = 0; i < 100; i++) {
      recycler.getOrCreate(newMessage(i).asMessageKey());
    }

    LinkedList<SizeBoundedQueue> shrink = new LinkedList<>();
    for (int i = 0; i < 100; i++) {
      shrink.add(recycler.lease(1, TimeUnit.MILLISECONDS));
    }
    TimeUnit.MILLISECONDS.sleep(10);
    recycler.shrink();
    for (SizeBoundedQueue queue : shrink) {
      recycler.recycle(queue);
    }

    CountDownLatch countDown = new CountDownLatch(1);
    new Thread(() -> {
      assertNull(recycler.lease(5, TimeUnit.MILLISECONDS));
      countDown.countDown();
    }).run();

    TimeUnit.MILLISECONDS.sleep(6);
    recycler.getOrCreate(newMessage(1000).asMessageKey());
    countDown.await();
  }

  @Test
  public void shouldLeasedIfNotEmptyEvenAfterKeepalive() throws InterruptedException {
    QueueRecycler recycler = newRecycler();
    recycler.getOrCreate(newMessage(0).asMessageKey());
    SizeBoundedQueue queue = recycler.lease(1, TimeUnit.MILLISECONDS);
    queue.offer(newMessage(0), newDeferred());
    TimeUnit.MILLISECONDS.sleep(10);
    assertEquals(0, recycler.shrink().size());
    recycler.recycle(queue);
    assertEquals(queue, recycler.lease(1, TimeUnit.MILLISECONDS));
    queue.drainTo(next -> true);
    assertNull(recycler.lease(1, TimeUnit.MILLISECONDS));
  }
}
