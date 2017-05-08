package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jdeferred.impl.DeferredObject;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by guohang.bao on 2017/5/4.
 */
@SuppressWarnings("unchecked")
public class SizeFirstQueueRecyclerTest {

  SizeFirstQueueRecycler recycler = new SizeFirstQueueRecycler(5, Strategy.Fail,
      TimeUnit.MILLISECONDS.toNanos(10));

  @Before
  public void setup() {
    recycler.clear();
  }

  static DeferredObject newDeferred() {
    return new DeferredObject();
  }

  @Test
  public void getAfterCreate() {
    SizeBoundedQueue q = recycler.getOrCreate(Message.STRICT_ORDER);
    assertEquals(q, recycler.getOrCreate(Message.STRICT_ORDER));
  }

  @Test
  public void leaseBySize() {
    SizeBoundedQueue q0 = recycler.getOrCreate(newMessage(0).asMessageKey()); // size 0

    SizeBoundedQueue q2 = recycler.getOrCreate(newMessage(2).asMessageKey()); // size 2
    q2.offer(newMessage(2), newDeferred());
    q2.offer(newMessage(2), newDeferred());

    SizeBoundedQueue q1 = recycler.getOrCreate(newMessage(1).asMessageKey()); // size 1
    q1.offer(newMessage(1), newDeferred());

    // reorder
    for (int i = 0; i < 3; i++) {
      SizeBoundedQueue leased = recycler.lease(1, TimeUnit.MILLISECONDS);
      recycler.recycle(leased);
    }

    assertEquals(q2, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q1, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q0, recycler.lease(1, TimeUnit.MILLISECONDS));
  }

  @Test
  public void keepAlive() throws InterruptedException {
    SizeBoundedQueue q = recycler.getOrCreate(Message.STRICT_ORDER);
    TimeUnit.MILLISECONDS.sleep(10);

    recycler.shrink();
    assertEquals(0, recycler.bySize.size());
    assertEquals(0, recycler.keyToQueue.size());
    assertEquals(0, recycler.keyToLastGet.size());
    SizeBoundedQueue q1 = recycler.getOrCreate(Message.STRICT_ORDER);
    assertNotEquals(q, q1);

    TimeUnit.MILLISECONDS.sleep(10);
    recycler.getOrCreate(Message.STRICT_ORDER);
    assertEquals(1, recycler.bySize.size());
    assertEquals(1, recycler.keyToQueue.size());
    assertEquals(1, recycler.keyToLastGet.size());
    assertEquals(q1, recycler.getOrCreate(Message.STRICT_ORDER));
  }

  @Test
  public void notLeaseShouldDieQueue() throws InterruptedException {
    recycler.getOrCreate(newMessage(0).asMessageKey());
    SizeBoundedQueue q = recycler.lease(1, TimeUnit.MILLISECONDS);
    TimeUnit.MILLISECONDS.sleep(10);
    recycler.shrink();
    recycler.recycle(q);
    assertNull(recycler.lease(1, TimeUnit.MILLISECONDS));
  }

  @Test
  public void leaseTimeout() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      recycler.getOrCreate(newMessage(i).asMessageKey());
    }
    for (int i = 0; i < 100; i++) {
      recycler.lease(1, TimeUnit.MILLISECONDS);
    }
    TimeUnit.MILLISECONDS.sleep(10);
    LinkedList<SizeBoundedQueue> shrink = recycler.shrink();
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
    recycler.getOrCreate(newMessage(0).asMessageKey());
    SizeBoundedQueue queue = recycler.lease(1, TimeUnit.MILLISECONDS);
    queue.offer(newMessage(0), newDeferred());
    TimeUnit.MILLISECONDS.sleep(10);
    recycler.shrink();
    recycler.recycle(queue);
    assertEquals(queue, recycler.lease(1, TimeUnit.MILLISECONDS));
    queue.drainTo(next -> true, 1);
    assertNull(recycler.lease(1, TimeUnit.MILLISECONDS));
  }
}
