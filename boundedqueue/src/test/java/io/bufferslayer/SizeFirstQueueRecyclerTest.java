package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.TimeUnit;
import org.jdeferred.impl.DeferredObject;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by guohang.bao on 2017/5/4.
 */
public class SizeFirstQueueRecyclerTest {

  SizeFirstQueueRecycler recycler = new SizeFirstQueueRecycler(1, Strategy.Fail,
      TimeUnit.MILLISECONDS.toNanos(10));

  @Before
  public void setup() {
    recycler.clear();
  }

  static SizeBoundedQueue newQueue() {
    return new SizeBoundedQueue(10, Strategy.Fail);
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
    SizeBoundedQueue q0 = newQueue(); // size 0
    recycler.recycle(q0);

    SizeBoundedQueue q2 = newQueue(); // size 2
    q2.offer(newMessage(0), newDeferred());
    q2.offer(newMessage(0), newDeferred());
    recycler.recycle(q2);

    SizeBoundedQueue q1 = newQueue(); // size 1
    q1.offer(newMessage(0), newDeferred());
    recycler.recycle(q1);

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
}
