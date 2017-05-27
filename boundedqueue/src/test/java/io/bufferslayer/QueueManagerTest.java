package io.bufferslayer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import io.bufferslayer.OverflowStrategy.Strategy;
import io.bufferslayer.QueueManager.Callback;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by tramchamploo on 2017/5/19.
 */
@SuppressWarnings("unchecked")
public class QueueManagerTest {

  QueueManager manager;

  @Before
  public void setup() {
    manager = new QueueManager(5, Strategy.Fail,
        TimeUnit.MILLISECONDS.toNanos(10));
  }
  
  @Test
  public void getAfterCreate() {
    SizeBoundedQueue q = manager.getOrCreate(Message.STRICT_ORDER);
    assertEquals(q, manager.getOrCreate(Message.STRICT_ORDER));
  }

  @Test
  public void callbackTriggeredAfterCreated() throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();
    manager.createCallback(new Callback() {
      @Override
      public void call(SizeBoundedQueue queue) {
        assertEquals(Message.STRICT_ORDER, queue.key);
        callCount.incrementAndGet();
        countDown.countDown();
      }
    });
    manager.getOrCreate(Message.STRICT_ORDER);
    countDown.await();
    manager.getOrCreate(Message.STRICT_ORDER);
    // only triggered once created
    assertEquals(1, callCount.get());
  }

  @Test
  public void keepAlive() throws InterruptedException {
    SizeBoundedQueue q = manager.getOrCreate(Message.STRICT_ORDER);
    TimeUnit.MILLISECONDS.sleep(10);

    assertEquals(Message.STRICT_ORDER, manager.shrink().get(0));
    assertEquals(0, manager.keyToQueue.size());
    assertEquals(0, manager.keyToLastGet.size());
    SizeBoundedQueue q1 = manager.getOrCreate(Message.STRICT_ORDER);
    assertNotEquals(q, q1);

    TimeUnit.MILLISECONDS.sleep(10);
    manager.getOrCreate(Message.STRICT_ORDER);
    assertEquals(1, manager.keyToQueue.size());
    assertEquals(1, manager.keyToLastGet.size());
    assertEquals(q1, manager.getOrCreate(Message.STRICT_ORDER));
  }
}
