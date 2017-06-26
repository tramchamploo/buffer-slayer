package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.QueueManager.Callback;
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
    SizeBoundedQueue q = manager.getOrCreate(Message.SINGLE_KEY);
    assertEquals(q, manager.getOrCreate(Message.SINGLE_KEY));
  }

  @Test
  public void callbackTriggeredAfterCreated() throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();
    manager.onCreate(new Callback() {
      @Override
      public void call(SizeBoundedQueue queue) {
        assertEquals(Message.SINGLE_KEY, queue.key);
        callCount.incrementAndGet();
        countDown.countDown();
      }
    });
    manager.getOrCreate(Message.SINGLE_KEY);
    countDown.await();
    manager.getOrCreate(Message.SINGLE_KEY);
    // only triggered once created
    assertEquals(1, callCount.get());
  }

  @Test
  public void keepAlive() throws InterruptedException {
    SizeBoundedQueue q = manager.getOrCreate(Message.SINGLE_KEY);
    TimeUnit.MILLISECONDS.sleep(10);

    assertEquals(Message.SINGLE_KEY, manager.shrink().get(0));
    assertEquals(0, manager.keyToQueue.size());
    assertEquals(0, manager.keyToLastGet.size());
    SizeBoundedQueue q1 = manager.getOrCreate(Message.SINGLE_KEY);
    assertNotEquals(q, q1);

    TimeUnit.MILLISECONDS.sleep(10);
    manager.getOrCreate(Message.SINGLE_KEY);
    assertEquals(1, manager.keyToQueue.size());
    assertEquals(1, manager.keyToLastGet.size());
    assertEquals(q1, manager.getOrCreate(Message.SINGLE_KEY));
  }
}
